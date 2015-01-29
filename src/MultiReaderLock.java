public class MultiReaderLock {

	// TDO: Add any necessary members here.

	private int readers;
	private int writers;

	/**
	 * Initializes a multi-reader (single-writer) lock.
	 */
	public MultiReaderLock() {
		// TODO: Initialize members.
		readers = 0;
		writers = 0;
	}

	public int getReaders() {
		return readers;
	}

	public int getWriters() {
		return writers;
	}

	/**
	 * Will wait until there are no active writers in the system, and then will
	 * increase the number of active readers.
	 */
	public synchronized void lockRead() {
		// TODO: Fill in. Do not modify method signature.
		while (writers > 0) {
			try {
				this.wait();
			} catch (InterruptedException ex) {
				System.out.println(ex);
			}
		}
		readers++;
	}

	/**
	 * Will decrease the number of active readers, and notify any waiting
	 * threads if necessary.
	 */
	public synchronized void unlockRead() {
		// TODO: Fill in. Do not modify method signature.
		readers--;
		notifyAll();

	}

	/**
	 * Will wait until there are no active readers or writers in the system, and
	 * then will increase the number of active writers.
	 */
	public synchronized void lockWrite() {
		// TODO: Fill in. Do not modify method signature.
		while (writers > 0 || readers > 0) {
			try {
				this.wait();
			} catch (InterruptedException ex) {
				System.out.println(ex);
			}
		}
		writers++;
	}

	/**
	 * Will decrease the number of active writers, and notify any waiting
	 * threads if necessary.
	 */
	public synchronized void unlockWrite() {
		// TODO: Fill in. Do not modify method signature.
		writers--;
		notifyAll();

	}

}
