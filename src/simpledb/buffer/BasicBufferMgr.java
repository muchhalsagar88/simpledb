package simpledb.buffer;

import java.util.HashMap;
import java.util.Map;

import simpledb.file.*;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {

	//TODO : Replace this by the 
	private Buffer[] bufferpool;
	private Map<Block, Buffer> bufferPoolMap;

	public Map<Block, Buffer> getBufferPoolMap() {
		return bufferPoolMap;
	}

	private int numAvailable;

	/**
	 * Creates a buffer manager having the specified number 
	 * of buffer slots.
	 * This constructor depends on both the {@link FileMgr} and
	 * {@link simpledb.log.LogMgr LogMgr} objects 
	 * that it gets from the class
	 * {@link simpledb.server.SimpleDB}.
	 * Those objects are created during system initialization.
	 * Thus this constructor cannot be called until 
	 * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
	 * is called first.
	 * @param numbuffs the number of buffer slots to allocate
	 */
	BasicBufferMgr(int numbuffs) {
		bufferpool = new Buffer[numbuffs];
		bufferPoolMap = new HashMap<Block, Buffer>(numbuffs);
		numAvailable = numbuffs;
		for (int i=0; i<numbuffs; i++)
			bufferpool[i] = new Buffer();
	}

	/**
	 * Flushes the dirty buffers modified by the specified transaction.
	 * @param txnum the transaction's id number
	 */
	synchronized void flushAll(int txnum) {
		for (Buffer buff : bufferpool)
			if (buff.isModifiedBy(txnum)){
				//bufferPoolMap.remove(buff.block());
				buff.flush();
			}
	}

	/**
	 * Pins a buffer to the specified block. 
	 * If there is already a buffer assigned to that block
	 * then that buffer is used;  
	 * otherwise, an unpinned buffer from the pool is chosen.
	 * Returns a null value if there are no available buffers.
	 * @param blk a reference to a disk block
	 * @return the pinned buffer
	 */
	synchronized Buffer pin(Block blk) {
		Block oldBlock = null;
		Buffer buff = findExistingBuffer(blk);
		if (buff == null) {
			buff = chooseUnpinnedBuffer();
			if (buff == null)
				return null;
			oldBlock = buff.block();
			buff.assignToBlock(blk);
			bufferPoolMap.remove(oldBlock);
			bufferPoolMap.put(blk, buff);
		}
		if (!buff.isPinned()){
			numAvailable--;
		}
		buff.pin();
		return buff;
	}

	/**
	 * Allocates a new block in the specified file, and
	 * pins a buffer to it. 
	 * Returns null (without allocating the block) if 
	 * there are no available buffers.
	 * @param filename the name of the file
	 * @param fmtr a pageformatter object, used to format the new block
	 * @return the pinned buffer
	 */
	synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
		Buffer buff = chooseUnpinnedBuffer();
		if (buff == null)
			return null;
		bufferPoolMap.remove(buff.block());
		buff.assignToNew(filename, fmtr);
		bufferPoolMap.put(buff.block(), buff);
		numAvailable--;
		buff.pin();
		return buff;
	}

	/**
	 * Unpins the specified buffer.
	 * @param buff the buffer to be unpinned
	 */
	synchronized void unpin(Buffer buff) {

		buff.unpin();
		if (!buff.isPinned()){
			numAvailable++;
			//bufferPoolMap.remove(buff.block());
		}
	}

	/**
	 * Returns the number of available (i.e. unpinned) buffers.
	 * @return the number of available buffers
	 */
	int available() {
		return numAvailable;
	}

	private Buffer findExistingBuffer(Block blk) {

		return bufferPoolMap.get(blk);
	}

	private Buffer chooseUnpinnedBuffer() {
		// Find the 1st unpinned buffer and return it..
		Buffer retBuff = null;
		System.out.println("-----------------------------------------------NUMAVLBLE:"+numAvailable);
		if(numAvailable > 0){
			for (Buffer buff : bufferpool)
				if (!buff.isPinned()){
					retBuff = buff;
					break;
				}

		}
		else{
			int leastLsn = 99999999;

			for (Buffer buff : bufferpool)
				if(!buff.isPinned() && buff.getModifiedBy()!= -1 && buff.getLogSequenceNumber()<leastLsn){
					leastLsn = buff.getLogSequenceNumber();
					retBuff=buff;
				}
			if(retBuff==null){
				for (Buffer buff : bufferpool)
					if(!buff.isPinned() && buff.getLogSequenceNumber()>-1 && buff.getLogSequenceNumber()<leastLsn){
						leastLsn = buff.getLogSequenceNumber();
						retBuff=buff;
					}

			}

		}
		return retBuff;

	}
	
	public void getStatistics() {
		
		StringBuilder builder = new StringBuilder();
		for(int i=0; i<bufferpool.length; ++i) {
			Buffer buffer = bufferpool[i];
			
			builder.append("For buffer number: ").append(i+1).append('\t').append("Reads: ").append(buffer.getReads())
				.append('\t').append(" Writes: ").append(buffer.getWrites()).append('\n');
		}
		System.out.println(builder.toString());
	}
}
