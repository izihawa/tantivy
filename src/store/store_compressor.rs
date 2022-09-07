use std::io::Write;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::{io, thread};

use common::{BinarySerializable, CountingWriter, HasLen, TerminatingWrite};
use lockfree_object_pool::{LinearObjectPool, LinearOwnedReusable};
use rayon::{ThreadPool, ThreadPoolBuilder};

use super::DOC_STORE_VERSION;
use crate::directory::WritePtr;
use crate::store::footer::DocStoreFooter;
use crate::store::index::{Checkpoint, SkipIndexBuilder};
use crate::store::{Compressor, Decompressor, StoreReader};
use crate::DocId;

pub struct BlockCompressor {
    compressor: Compressor,
    variant: BlockCompressorVariants,
}

// The struct wrapping an enum is just here to keep the
// impls private.
enum BlockCompressorVariants {
    SameThread(BlockWriter),
    DedicatedThread(DedicatedThreadBlockCompressorImpl),
}

impl BlockCompressor {
    pub fn new(compressor: Compressor, wrt: WritePtr, threads: usize) -> io::Result<Self> {
        let block_compressor_impl = BlockWriter::new(wrt);
        if threads >= 1 {
            let dedicated_thread_compressor = DedicatedThreadBlockCompressorImpl::new(
                compressor,
                block_compressor_impl,
                threads,
            )?;
            Ok(BlockCompressor {
                compressor,
                variant: BlockCompressorVariants::DedicatedThread(dedicated_thread_compressor),
            })
        } else {
            Ok(BlockCompressor {
                compressor,
                variant: BlockCompressorVariants::SameThread(block_compressor_impl),
            })
        }
    }

    pub fn compress_block_and_write(
        &mut self,
        bytes: &[u8],
        num_docs_in_block: u32,
    ) -> io::Result<()> {
        assert!(num_docs_in_block > 0);
        match &mut self.variant {
            BlockCompressorVariants::SameThread(block_writer) => {
                let mut intermediary_buffer = Vec::with_capacity(bytes.len());
                self.compressor
                    .compress_into(bytes, &mut intermediary_buffer)?;
                block_writer.write_data(&intermediary_buffer, num_docs_in_block)?;
            }
            BlockCompressorVariants::DedicatedThread(different_thread_block_compressor) => {
                different_thread_block_compressor.compress_block_and_write(
                    self.compressor,
                    bytes,
                    num_docs_in_block,
                )?;
            }
        }
        Ok(())
    }

    pub fn stack_reader(&mut self, store_reader: StoreReader) -> io::Result<()> {
        match &mut self.variant {
            BlockCompressorVariants::SameThread(block_writer) => {
                block_writer.stack(store_reader)?;
            }
            BlockCompressorVariants::DedicatedThread(different_thread_block_compressor) => {
                different_thread_block_compressor.stack_reader(store_reader)?;
            }
        }
        Ok(())
    }

    pub fn close(self) -> io::Result<()> {
        let imp = self.variant;
        match imp {
            BlockCompressorVariants::SameThread(block_writer) => {
                block_writer.close(Decompressor::from(self.compressor))
            }
            BlockCompressorVariants::DedicatedThread(different_thread_block_compressor) => {
                different_thread_block_compressor.close()
            }
        }
    }
}

struct BlockWriter {
    first_doc_in_block: DocId,
    offset_index_writer: SkipIndexBuilder,
    writer: CountingWriter<WritePtr>,
}

impl BlockWriter {
    fn new(writer: WritePtr) -> Self {
        Self {
            first_doc_in_block: 0,
            offset_index_writer: SkipIndexBuilder::new(),
            writer: CountingWriter::wrap(writer),
        }
    }

    fn write_block(&mut self, compressed_block: CompressedBlock) -> io::Result<()> {
        self.write_data(
            &compressed_block.compressed_block_data,
            compressed_block.num_docs_in_block,
        )
    }

    fn write_data(&mut self, compressed_block: &[u8], num_docs_in_block: u32) -> io::Result<()> {
        assert!(num_docs_in_block > 0);

        let start_offset = self.writer.written_bytes();
        self.writer.write_all(compressed_block)?;
        let end_offset = self.writer.written_bytes();

        self.register_checkpoint(Checkpoint {
            doc_range: self.first_doc_in_block..self.first_doc_in_block + num_docs_in_block,
            byte_range: start_offset..end_offset,
        });
        Ok(())
    }

    fn register_checkpoint(&mut self, checkpoint: Checkpoint) {
        self.offset_index_writer.insert(checkpoint.clone());
        self.first_doc_in_block = checkpoint.doc_range.end;
    }

    /// Stacks a store reader on top of the documents written so far.
    /// This method is an optimization compared to iterating over the documents
    /// in the store and adding them one by one, as the store's data will
    /// not be decompressed and then recompressed.
    fn stack(&mut self, store_reader: StoreReader) -> io::Result<()> {
        let doc_shift = self.first_doc_in_block;
        let start_shift = self.writer.written_bytes();

        // just bulk write all of the block of the given reader.
        self.writer
            .write_all(store_reader.block_data()?.as_slice())?;

        // concatenate the index of the `store_reader`, after translating
        // its start doc id and its start file offset.
        for mut checkpoint in store_reader.block_checkpoints() {
            checkpoint.doc_range.start += doc_shift;
            checkpoint.doc_range.end += doc_shift;
            checkpoint.byte_range.start += start_shift;
            checkpoint.byte_range.end += start_shift;
            self.register_checkpoint(checkpoint);
        }
        Ok(())
    }

    fn close(mut self, decompressor: Decompressor) -> io::Result<()> {
        let header_offset: u64 = self.writer.written_bytes();
        let docstore_footer = DocStoreFooter::new(header_offset, decompressor, DOC_STORE_VERSION);
        self.offset_index_writer.serialize_into(&mut self.writer)?;
        docstore_footer.serialize(&mut self.writer)?;
        self.writer.terminate()
    }
}

struct CompressionPool {
    thread_pool: ThreadPool,
    memory_pool: Arc<LinearObjectPool<Vec<u8>>>,
}

impl CompressionPool {
    pub fn new(threads: usize) -> Self {
        CompressionPool {
            thread_pool: ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap(),
            memory_pool: Arc::new(LinearObjectPool::new(|| vec![], |x| x.clear())),
        }
    }

    pub fn compress(
        &self,
        compressor: Compressor,
        block_data: &[u8],
        num_docs_in_block: u32,
    ) -> oneshot::Receiver<CompressedBlock> {
        let (sender, receiver) = oneshot::channel();
        let mut intermediary_buffer = self.memory_pool.pull_owned();
        let block_data = block_data.to_vec();
        self.thread_pool.spawn(move || {
            compressor
                .compress_into(&block_data, &mut intermediary_buffer)
                .unwrap();
            sender
                .send(CompressedBlock::new(intermediary_buffer, num_docs_in_block))
                .unwrap();
        });
        receiver
    }
}

struct CompressedBlock {
    compressed_block_data: LinearOwnedReusable<Vec<u8>>,
    num_docs_in_block: u32,
}

impl CompressedBlock {
    pub fn new(
        compressed_block_data: LinearOwnedReusable<Vec<u8>>,
        num_docs_in_block: u32,
    ) -> Self {
        CompressedBlock {
            compressed_block_data,
            num_docs_in_block,
        }
    }
}

// ---------------------------------
enum BlockCompressorMessage {
    CompressionHandler(oneshot::Receiver<CompressedBlock>),
    Stack(StoreReader),
}

struct DedicatedThreadBlockCompressorImpl {
    join_handle: Option<JoinHandle<io::Result<()>>>,
    tx: SyncSender<BlockCompressorMessage>,
    compression_pool: CompressionPool,
}

impl DedicatedThreadBlockCompressorImpl {
    fn new(
        compressor: Compressor,
        mut block_writer: BlockWriter,
        threads: usize,
    ) -> io::Result<Self> {
        let (tx, rx): (
            SyncSender<BlockCompressorMessage>,
            Receiver<BlockCompressorMessage>,
        ) = sync_channel(threads);
        let join_handle = thread::Builder::new()
            .name("docstore-compressor-thread".to_string())
            .spawn(move || {
                while let Ok(packet) = rx.recv() {
                    match packet {
                        BlockCompressorMessage::CompressionHandler(compression_handler) => {
                            block_writer.write_block(compression_handler.recv().unwrap())?;
                        }
                        BlockCompressorMessage::Stack(store_reader) => {
                            block_writer.stack(store_reader)?;
                        }
                    }
                }
                block_writer.close(Decompressor::from(compressor))?;
                Ok(())
            })?;
        Ok(DedicatedThreadBlockCompressorImpl {
            join_handle: Some(join_handle),
            tx,
            compression_pool: CompressionPool::new(threads),
        })
    }

    fn compress_block_and_write(
        &mut self,
        compressor: Compressor,
        bytes: &[u8],
        num_docs_in_block: u32,
    ) -> io::Result<()> {
        let compression_handler =
            self.compression_pool
                .compress(compressor, bytes, num_docs_in_block);
        self.send(BlockCompressorMessage::CompressionHandler(
            compression_handler,
        ))
    }

    fn stack_reader(&mut self, store_reader: StoreReader) -> io::Result<()> {
        self.send(BlockCompressorMessage::Stack(store_reader))
    }

    fn send(&mut self, msg: BlockCompressorMessage) -> io::Result<()> {
        if self.tx.send(msg).is_err() {
            harvest_thread_result(self.join_handle.take())?;
            return Err(io::Error::new(io::ErrorKind::Other, "Unidentified error."));
        }
        Ok(())
    }

    fn close(self) -> io::Result<()> {
        drop(self.tx);
        harvest_thread_result(self.join_handle)
    }
}

/// Wait for the thread result to terminate and returns its result.
///
/// If the thread panicked, or if the result has already been harvested,
/// returns an explicit error.
fn harvest_thread_result(join_handle_opt: Option<JoinHandle<io::Result<()>>>) -> io::Result<()> {
    let join_handle = join_handle_opt
        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Thread already joined."))?;
    join_handle
        .join()
        .map_err(|_err| io::Error::new(io::ErrorKind::Other, "Compressing thread panicked."))?
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::path::Path;

    use crate::directory::RamDirectory;
    use crate::store::store_compressor::BlockCompressor;
    use crate::store::Compressor;
    use crate::Directory;

    fn populate_block_compressor(mut block_compressor: BlockCompressor) -> io::Result<()> {
        block_compressor.compress_block_and_write(b"hello", 1)?;
        block_compressor.compress_block_and_write(b"happy", 1)?;
        block_compressor.close()?;
        Ok(())
    }

    #[test]
    fn test_block_store_compressor_impls_yield_the_same_result() {
        let ram_directory = RamDirectory::default();
        let path1 = Path::new("path1");
        let path2 = Path::new("path2");
        let wrt1 = ram_directory.open_write(path1).unwrap();
        let wrt2 = ram_directory.open_write(path2).unwrap();
        let block_compressor1 = BlockCompressor::new(Compressor::None, wrt1, 1).unwrap();
        let block_compressor2 = BlockCompressor::new(Compressor::None, wrt2, 0).unwrap();
        populate_block_compressor(block_compressor1).unwrap();
        populate_block_compressor(block_compressor2).unwrap();
        let data1 = ram_directory.open_read(path1).unwrap();
        let data2 = ram_directory.open_read(path2).unwrap();
        assert_eq!(data1.read_bytes().unwrap(), data2.read_bytes().unwrap());
    }
}
