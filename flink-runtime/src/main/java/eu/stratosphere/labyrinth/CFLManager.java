/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eu.stratosphere.labyrinth;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ConnectException;
import java.net.MulticastSocket;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

/**
 *
 */
public class CFLManager {

	protected static final Logger LOG = LoggerFactory.getLogger(CFLManager.class);

	private static final boolean logCoord = false;

	public static boolean barrier = false; // barrier between iteration steps

	private static final int port = 4444;
	//private static final int UDPPort = port + 1;

	public byte tmId = -1;
	public int numAllSlots = -1;
	public int numTaskSlotsPerTm = -1;

	public CFLManager(TaskManager tm, String[] hosts, boolean coordinator) {
		this.tm = tm;
		this.hosts = hosts;
		this.coordinator = coordinator;

//		recvdSeqNums = new SeqNumAtomicBools(16384);
//
//		try {
//			UDPSocket = new MulticastSocket(UDPPort);
//			multicastGroup = InetAddress.getByName("229.8.9.10");
//			UDPSocket.joinGroup(multicastGroup);
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}
//
//		udpReceiver = new UDPReceiver(); //thread
		new ConnAccepter(); //thread

		senderStreams = new OutputStream[hosts.length];
		senderDataOutputViews = new DataOutputViewStreamWrapper[hosts.length];

		cflSendSeqNum = 0;

		jobCounter = 0;

		createSenderConnections();
	}

	private TaskManager tm;

	private boolean coordinator;

	private String[] hosts;

	private OutputStream[] senderStreams;
	private DataOutputViewStreamWrapper[] senderDataOutputViews;

	private volatile boolean allSenderUp = false;
	private volatile boolean allIncomingUp = false;

//	private final MulticastSocket UDPSocket;
//	private final InetAddress multicastGroup;
//	private final int UDPMaxPacketSize = 32;
//	private final UDPReceiver udpReceiver;
//	private final SeqNumAtomicBools recvdSeqNums;

	private List<Integer> tentativeCFL = new ArrayList<>(); // ez lehet lyukas, ha nem sorrendben erkeznek meg az elemek
	private List<Integer> curCFL = new ArrayList<>(); // ez sosem lyukas // could be changed to fastutils IntArrayList

	private List<CFLCallback> callbacks = new ArrayList<>();

	private int terminalBB = -1;
	private int numSubscribed = 0;
	private Integer numToSubscribe = null;

	private volatile int cflSendSeqNum = 0;

	// https://stackoverflow.com/questions/6012640/locking-strategies-and-techniques-for-preventing-deadlocks-in-code
	// We obtain multiple locks only in the following orders:
	// ! This might be stale since the workQueue refactoring
	// When receiving a msg:
	//   CFLManager -> BagOperatorHost -> msgSendLock  or
	//   CFLManager -> msgSendLock  (when the msg triggers sending closeInputBag)
	// When entering through processElement:
	//   BagOperatorHost -> msgSendLock
	private final Object msgSendLock = new Object();

	private volatile short jobCounter = -10;

	public JobID getJobID() {
		return jobID;
	}

	public void setJobID(JobID jobID) {
		LOG.info("CFLManager.setJobID to '" + jobID + "'");
		if (this.jobID != null && !this.jobID.equals(jobID) && jobID != null) {
			throw new RuntimeException("Csak egy job futhat egyszerre. (old: " + this.jobID + ", new: " + jobID + ")");
			// amugy ez akkor is triggerelodik, ha nem CFL-es jobok akar csak egymas utan tobb akarna futni
		}
		this.jobID = jobID;
	}

	private JobID jobID = null;

	private void createSenderConnections() {
		final int timeout = 500;
		int i = 0;
		for (String host : hosts) {
			try {
				Socket socket;
				while(true) {
					try {
						socket = new Socket();
						socket.setPerformancePreferences(0,1,0);
						LOG.info("Connecting sender connection to " + host + ".");
						socket.connect(new InetSocketAddress(host, port), timeout);
						LOG.info("Sender connection connected to  " + host + ".");
						break;
					} catch (SocketTimeoutException exTimeout) {
						LOG.info("Sender connection to            " + host + " timed out, retrying...");
					} catch (ConnectException ex) {
						LOG.info("Sender connection to            " + host + " was refused, retrying...");
						try {
							Thread.sleep(500);
						} catch (InterruptedException e) {
							throw new RuntimeException(e);
						}
					} catch (IOException e) {
						LOG.info("Sender connection to            " + host + " caused an IOException, retrying... " + e);
						try {
							Thread.sleep(500);
						} catch (InterruptedException e2) {
							throw new RuntimeException(e2);
						}
					}
				}
				senderStreams[i] = new BufferedOutputStream(socket.getOutputStream(), 32768);
				senderDataOutputViews[i] = new DataOutputViewStreamWrapper(senderStreams[i]);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			i++;
		}
		LOG.info("All sender connections are up.");
		allSenderUp = true;
	}

	private void sendElement(CFLElement e) {
		try {
			final Msg msg = new Msg(jobCounter, e);

//			// UDP:
//			final DataOutputSerializer dos = new DataOutputSerializer(UDPMaxPacketSize);
//			msg.serialize(dos);
//			assert dos.length() <= UDPMaxPacketSize; // DataOutputSerializer would allow more (with resizing), but we have a fixed buffer for recv
//			UDPSocket.send(new DatagramPacket(dos.getSharedBuffer(), dos.length(), multicastGroup, UDPPort));

			// TCP:
			for (int i = 0; i < hosts.length; i++) {
				msg.serialize(senderDataOutputViews[i]);
				senderStreams[i].flush();
			}
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}
	}

//	private class UDPReceiver implements Runnable {
//
//		private volatile boolean stopped = false;
//
//		UDPReceiver() {
//			Thread thread = new Thread(this, "UDPReceiver");
//			thread.setDaemon(true);
//			thread.start();
//		}
//
//		@Override
//		public void run() {
//			try {
//				byte[] buf = new byte[UDPMaxPacketSize];
//				while (!stopped) {
//					DatagramPacket packet = new DatagramPacket(buf, buf.length);
//					UDPSocket.receive(packet);
//					if (stopped) break;
//					DataInputDeserializer dids = new DataInputDeserializer(buf);
//
//					// Vigyazat, itt lenyeges, hogy a reuse minden fieldje null!
//					Msg msg = new Msg();
//					Msg.deserialize(msg, dids);
//					//msg.assertOK();
//
//					if (logCoord && LOG.isInfoEnabled()) {
//						LOG.info("Received UDP msg " + msg + "; I am " + this.toString());
//					}
//
//					assert msg.cflElement != null;
//
//					if (!recvdSeqNums.getAndSet(msg.cflElement.seqNum)) {
//						addTentative(msg.cflElement.seqNum, msg.cflElement.bbId);
//					}
//				}
//			} catch (Throwable t) {
//				t.printStackTrace();
//				LOG.error(ExceptionUtils.stringifyException(t));
//				Runtime.getRuntime().halt(300);
//			}
//		}
//
//		public void stop() {
//			stopped = true;
//			try {
//				UDPSocket.leaveGroup(multicastGroup);
//			} catch (IOException e) {
//				throw new RuntimeException(e);
//			}
//		}
//	}

	private class ConnAccepter implements Runnable {

		ConnAccepter() {
			new Thread(this, "ConnAccepter").start();
		}

		@Override
		public void run() {
			ServerSocket serverSocket;
			try {
				serverSocket = new ServerSocket(port);
				int i = 0;
				while(i < hosts.length) {
					LOG.info("Listening for incoming connections " + i);
					Socket socket = serverSocket.accept();
					SocketAddress remoteAddr = socket.getRemoteSocketAddress();
					LOG.info("Got incoming connection " + i + " from " + remoteAddr);
					new ConnReader(socket, i);
					i++;
				}
				LOG.info("All incoming connections connected");
				allIncomingUp = true;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private class ConnReader implements Runnable {

		private final Socket socket;

		ConnReader(Socket socket, int connID) {
			this.socket = socket;
			Thread thread = new Thread(this, "ConnReader_" + connID);
			thread.setPriority(Thread.MAX_PRIORITY);
			thread.setDaemon(true);
			thread.start();
		}

		@Override
		public void run() {
			try {
				try {
					InputStream ins = new BufferedInputStream(socket.getInputStream(), 32768);
					DataInputViewStreamWrapper divsw = new DataInputViewStreamWrapper(ins);
					while (true) {

						// Vigyazat, itt lenyeges, hogy a reuse minden fieldje null!
						Msg msg = new Msg();
						Msg.deserialize(msg, divsw);
						//msg.assertOK();

						//synchronized (CFLManager.this) {
							if (logCoord) LOG.info("Got " + msg);

							if (msg.jobCounter < jobCounter) {
								// Mondjuk ebbol itt lehet baj, ha vki meg nem kapta meg nem kapta meg a voteStop-hoz eljutashoz szukseges msg-ket.
								// De amiota a coordinatoron osszevarjuk a vegzeseket, mielott barki is resetelne, azota remeljuk nem jutunk ide.
								LOG.info("Old msg, ignoring (msg.jobCounter = " + msg.jobCounter + ", jobCounter = " + jobCounter + ")");
								continue;
							}
							while (msg.jobCounter > jobCounter) {
								LOG.info("Too new msg, waiting (msg.jobCounter = " + msg.jobCounter + ", jobCounter = " + jobCounter + ")"); // Ilyenkor valojaban a resetre varunk
								Thread.sleep(100);
							}

							if (msg.cflElement != null) {
								//if (!recvdSeqNums.getAndSet(msg.cflElement.seqNum)) {
									addTentative(msg.cflElement.seqNum, msg.cflElement.bbId); // will do the callbacks
								//}
							} else if (msg.consumed != null) {
								assert coordinator;
								consumedRemote(msg.consumed.bagID, msg.consumed.numElements, msg.consumed.subtaskIndex, msg.consumed.opID);
							} else if (msg.produced != null) {
								assert coordinator;
								producedRemote(msg.produced.bagID, msg.produced.inpIDs, msg.produced.numElements, msg.produced.para, msg.produced.subtaskIndex, msg.produced.opID);
							} else if (msg.closeInputBag != null) {
								closeInputBagRemote(msg.closeInputBag.bagID, msg.closeInputBag.opID);
							} else if (msg.subscribeCnt != null) {
								subscribeCntRemote();
							} else if (msg.barrierAllReached != null) {
								barrierAllReachedRemote(msg.barrierAllReached.cflSize);
							} else if (msg.voteStop != null) {
								voteStopRemote();
							} else if (msg.stop != null) {
								stopRemote();
							} else {
								assert false;
							}
						//}
					}
				} catch (EOFException e) {
					// This happens when the other TM shuts down. No need to throw a RuntimeException here, as we are shutting down anyway.
				} catch (IOException e) {
					throw new RuntimeException(e);
				} finally {
					socket.close();
				}
			} catch (Throwable e) {
				e.printStackTrace();
				LOG.error(ExceptionUtils.stringifyException(e));
				Runtime.getRuntime().halt(200);
			}
		}
	}

	private synchronized void addTentative(int seqNum, int bbId) {
//		if (LOG.isInfoEnabled()) {
//			LOG.info("addTentative(" + seqNum + ", " + bbId + ")");
//		}

		while (seqNum >= tentativeCFL.size()) {
			tentativeCFL.add(null);
			// Vigyazat, a kov. if-et nem lehetne max-szal kivaltani, mert akkor osszeakadhatnank
			// az appendToCFL-ben levo inkrementalassal.
			if (seqNum + 1 > cflSendSeqNum) {
				cflSendSeqNum = seqNum + 1;
			}
		}
		assert tentativeCFL.get(seqNum) == null || tentativeCFL.get(seqNum).equals(bbId);
		tentativeCFL.set(seqNum, bbId);

		for (int i = curCFL.size(); i < tentativeCFL.size(); i++) {
			Integer t = tentativeCFL.get(i);
			if (t == null)
				break;
			curCFL.add(t);
			if (LOG.isInfoEnabled()) {
				LOG.info("Adding BBID " + t + " to CFL " + System.currentTimeMillis());
			}
			notifyCallbacks();
			// szoval minden elemnel kuldunk kulon, tehat a subscribereknek sok esetben eleg lehet az utolso elemet nezni
		}
	}

	private boolean shouldNotifyTerminalBB() {
		return curCFL.size() > 0 && curCFL.get(curCFL.size() - 1) == terminalBB;
	}

	private int prevCallbacksSize = -1;

	private synchronized void notifyCallbacks() {
		if (callbacks.size() != prevCallbacksSize) {
			prevCallbacksSize = callbacks.size();
			callbacks.sort(new Comparator<CFLCallback>() {
				@Override
				public int compare(CFLCallback o1, CFLCallback o2) {
					return -Integer.compare(o1.getOpID(), o2.getOpID());
				}
			});
		}

		ArrayList<Integer> CFLToPass = new ArrayList<>(curCFL);
		for (CFLCallback cb: callbacks) {
			cb.notify(CFLToPass);
		}

		assert callbacks.size() == 0 || terminalBB != -1; // A drivernek be kell allitania a job elindulasa elott. Viszont ebbe a fieldbe a BagOperatorHost.setup-ban kerul.
		if (shouldNotifyTerminalBB()) {
			assert terminalBB != -1;
			// We need to copy, because notifyTerminalBB might call unsubscribe, which would lead to a ConcurrentModificationException
			ArrayList<CFLCallback> origCallbacks = new ArrayList<>(callbacks);
			for (CFLCallback cb: origCallbacks) {
				cb.notifyTerminalBB();
			}
		}
	}


	// --------------------------------------------------------

	// A helyi TM-ben futo operatorok hivjak
	public void appendToCFL(int bbId) {
		synchronized (msgSendLock) {

			// Ugyebar ha valaki appendel a CFL-hez, akkor mindig biztos a dolgaban.
			// (Akkor is, ha tobbet appendel, mert olyan BB-ket is appendel, amelyekben nincs condition node.)
			// Szoval nem fordulhat elo olyan, hogy el akarna agazni a CFL.
			// Emiatt biztosak lehetunk benne, hogy nem akar senki olyankor appendelni, amikor meg nem
			// kapta meg a legutobbi CFL-t. Ebbol kovetkezik, hogy itt nem lehetnek lyukak.
			// Viszont ami miatt ezt megis ki kellett commentezni, az az, hogy szinkronizalasi problema van az
			// addTentative-val olyankor, amikor egymas utan tobb appendToCFL-t hiv valaki.
			//assert tentativeCFL.size() == curCFL.size();

			if (LOG.isInfoEnabled()) LOG.info("Adding " + bbId + " to CFL (appendToCFL) " + System.currentTimeMillis());
			sendElement(new CFLElement(cflSendSeqNum++, bbId));
		}
	}

	public synchronized void subscribe(CFLCallback cb) {
		LOG.info("CFLManager.subscribe");
		assert allIncomingUp && allSenderUp;

		// Maybe there could be a waitForReset here

		callbacks.add(cb);

		// Egyenkent elkuldjuk a notificationt mindegyik eddigirol
		List<Integer> tmpCfl = new ArrayList<>();
		for(Integer x: curCFL) {
			tmpCfl.add(x);
			ArrayList<Integer> CFLToPass = new ArrayList<>(tmpCfl);
			cb.notify(CFLToPass);
		}

		assert terminalBB != -1; // a drivernek be kell allitania a job elindulasa elott
		if (shouldNotifyTerminalBB()) {
			cb.notifyTerminalBB();
		}

		for(CloseInputBag cib: closeInputBags) {
			cb.notifyCloseInput(cib.bagID, cib.opID);
		}

		subscribeCntLocal();
	}

	private void checkVoteStop() {
		if (numToSubscribe != null && numSubscribed == numToSubscribe && callbacks.isEmpty()) {
			voteStopLocal();
		} else {
			if (numToSubscribe == null) {
				LOG.info("checkVoteStop: numToSubscribe == null");
			} else {
				if (callbacks.isEmpty() && numSubscribed != numToSubscribe) {
					LOG.info("checkVoteStop: callbacks.isEmpty(), but numSubscribed != numToSubscribe: " + numSubscribed + " != " + numToSubscribe);
				} else {
					LOG.info("checkVoteStop: callbacks has " + callbacks.size() + " elements");
				}
			}
		}
	}

	public synchronized void unsubscribe(CFLCallback cb) {
		LOG.info("CFLManager.unsubscribe");
		callbacks.remove(cb);

		checkVoteStop();
	}

	private synchronized void reset() {
		LOG.info("Resetting CFLManager.");

		assert callbacks.size() == 0;

		tentativeCFL.clear();
		curCFL.clear();

		cflSendSeqNum = 0;

		//recvdSeqNums.clear();

		bagStatuses.clear();
		bagConsumedStatuses.clear();
		emptyBags.clear();

		closeInputBags.clear();

		barrierReached.clear();

		terminalBB = -1;
		numSubscribed = 0;
		numToSubscribe = null;

		numVoteStops = 0;

		jobCounter++;
	}

	// We need to call this when the TM is shutting down, because
	// when the next TM comes up in the same JVM (while testing locally), we don't want the old CFLManager
	// to get the UDP packets from the new jobs.
	// However, we shouldn't call this in reset(), because udpReceiver is initialized only in the ctor, which
	// is not called during a reset.
	public void stop() {
		LOG.info("Stopping CFLManager");
		//udpReceiver.stop();
	}

	public synchronized void specifyTerminalBB(int bbId) {
		LOG.info("specifyTerminalBB: " + bbId);
		terminalBB = bbId;
	}

	public synchronized void specifyNumToSubscribe(int numToSubscribe) {
		LOG.info("specifyNumToSubscribe: " + numToSubscribe);
		this.numToSubscribe = numToSubscribe;
		checkVoteStop();
	}

    // --------------------------------------------------------

	private static final int maxPara = 400; // BitSet sizes

    private static final class BagStatus {

        public int numProduced = 0;
		public boolean produceClosed = false;

		public final BitSet producedSubtasks = new BitSet(maxPara);

		public final HashSet<BagID> inputs = new HashSet<>();
		public final HashSet<BagID> inputTo = new HashSet<>();
		public final ArrayList<BagID> inputToList = new ArrayList<>(16); // 1 might be better
		public final BitSet consumedBy = new BitSet();

		public short para = -2;
    }

	private static final class BagConsumptionStatus {

		public int numConsumed = 0;
		public boolean consumeClosed = false;

		public final BitSet consumedSubtasks = new BitSet(maxPara);
	}

    private final BagIdToObjectMap<BagStatus> bagStatuses = new BagIdToObjectMap<>();

	private final BagIDAndOpIDToObjectMap<BagConsumptionStatus> bagConsumedStatuses = new BagIDAndOpIDToObjectMap<>();

	private final Set<BagID> emptyBags = new HashSet<>();

	private final List<CloseInputBag> closeInputBags = new ArrayList<>();

	// -- Begin barrier stuff --
	private final Map<Integer, Integer> barrierReached = new HashMap<>(); // CFLSize -> Int: mely steppel hanyan vegeztek
	//private static final Set<Integer> opsInLoop = new HashSet<>(Arrays.asList(15,5,6,7,10,11,16)); // ConnectedComponents
	private static final Set<Integer> opsInLoop = new HashSet<>(Arrays.asList(2,3,4,5,6,7,8,9)); // ClickCountNoJoin
	// -- End   barrier stuff --

	private final AtomicInteger waitingInSendToCoordinator = new AtomicInteger(0);

	private void sendToCoordinator(Msg msg) {
		waitingInSendToCoordinator.incrementAndGet();
		synchronized (msgSendLock) {
			waitingInSendToCoordinator.decrementAndGet();
			try {
				msg.serialize(senderDataOutputViews[0]);
				if (waitingInSendToCoordinator.get() == 0) {
					senderStreams[0].flush();
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private void sendToEveryone(Msg msg) {
		synchronized (msgSendLock) {
			for (int i = 0; i < hosts.length; i++) {
				try {
					msg.serialize(senderDataOutputViews[i]);
					senderStreams[i].flush();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

    // kliens -> coordinator
    public void consumedLocal(BagID bagID, int numElements, short subtaskIndex, int opID) {
    	sendToCoordinator(new Msg(jobCounter, new Consumed(bagID, numElements, subtaskIndex, opID)));
    }

    private synchronized void consumedRemote(BagID bagID, int numElements, short subtaskIndex, int opID) {
		if (logCoord) LOG.info("consumedRemote(bagID = " + bagID + ", numElements = " + numElements + ", opID = " + opID + ")");

		// Get or init BagStatus
		BagStatus s = bagStatuses.get(bagID);
		if (s == null) {
			s = new BagStatus();
			bagStatuses.put(bagID, s);
		}

		// Get or init BagConsumptionStatus
		BagIDAndOpID key = new BagIDAndOpID(bagID, opID);
		BagConsumptionStatus c = bagConsumedStatuses.get(key);
		if (c == null) {
			c = new BagConsumptionStatus();
			bagConsumedStatuses.put(key, c);
		}

		assert !c.consumeClosed;

		s.consumedBy.set(opID);

		c.consumedSubtasks.set(subtaskIndex);

		c.numConsumed += numElements;

		checkForClosingConsumed(bagID, s, c, opID);

		for (BagID b: s.inputToList) {
			// Regen azert volt jo itt a -1, mert ilyenkor biztosan nem source. De mostmar nem csak source-nal hasznaljuk a para-t
			assert s.para != -2;
			checkForClosingProduced(b, bagStatuses.get(b), s.para, b.opID);
		}
    }

    private void checkForClosingConsumed(BagID bagID, BagStatus s, BagConsumptionStatus c, int opID) {
		assert !c.consumeClosed;
		if (s.produceClosed) {
			assert c.numConsumed <= s.numProduced; // (ennek belul kell lennie az if-ben mert kivul a reordering miatt nem biztos, hogy igaz)
			if (c.numConsumed == s.numProduced) {
				if (logCoord) LOG.info("checkForClosingConsumed(" + bagID + ", opID = " + opID + "): consumeClosed, because numConsumed = " + c.numConsumed + ", numProduced = " + s.numProduced);
				c.consumeClosed = true;
				closeInputBagLocal(bagID, opID);
			} else {
				if (logCoord) LOG.info("checkForClosingConsumed(" + bagID + ", opID = " + opID + "): needMore, because numConsumed = " + c.numConsumed + ", numProduced = " + s.numProduced);
			}
		}
	}

    // kliens -> coordinator
	public void producedLocal(BagID bagID, BagID[] inpIDs, int numElements, short para, short subtaskIndex, int opID) {
		assert inpIDs.length <= 2; // ha 0, akkor BagSource
		sendToCoordinator(new Msg(jobCounter, new Produced(bagID, inpIDs, numElements, para, subtaskIndex, opID)));
    }

    private synchronized void producedRemote(BagID bagID, BagID[] inpIDs, int numElements, short para, short subtaskIndex, int opID) {
		if (logCoord) LOG.info("producedRemote(bagID = " + bagID + ", numElements = " + numElements + ", opID = " + opID + ")");

		// Get or init BagStatus
		BagStatus s = bagStatuses.get(bagID);
		if (s == null) {
			s = new BagStatus();
			bagStatuses.put(bagID, s);
		}

		assert !s.produceClosed || numElements == 0;

		// Add to s.inputs, and add to the inputTos of the inputs
		for (BagID inp: inpIDs) {
			s.inputs.add(inp);

			BagStatus inpS = bagStatuses.get(inp);
			if (inpS == null) {
				inpS = new BagStatus();
				bagStatuses.put(inp, inpS);
			}
			if (inpS.inputTo.add(bagID)) {
				inpS.inputToList.add(bagID);
			}
		}
		assert s.inputs.size() <= 2;

		// Set para
		s.para = para;

		// Add to s.numProduced
		s.numProduced += numElements;

		// Add to s.producedSubtasks
		assert !s.producedSubtasks.get(subtaskIndex);
		s.producedSubtasks.set(subtaskIndex);

		checkForClosingProduced(bagID, s, para, opID);

//		for (Integer copID: s.consumedBy) {
//			checkForClosingConsumed(bagID, s, bagConsumedStatuses.get(new BagIDAndOpID(bagID, copID)), copID);
//		}

		final BagStatus sf = s;
		s.consumedBy.stream().forEach(new IntConsumer() {
			@Override
			public void accept(int copID) {
				checkForClosingConsumed(bagID, sf, bagConsumedStatuses.get(new BagIDAndOpID(bagID, copID)), copID);
			}
		});
    }

    private void checkForClosingProduced(BagID bagID, BagStatus s, short para, int opID) {
		if (s.produceClosed) {
			return; // Mondjuk ezt nem ertem, hogy hogy fordulhat elo
		}

		if (s.inputs.size() == 0) {
			// source, tehat mindenhonnan varunk
			assert para != -1;
			int totalProducedMsgs = s.producedSubtasks.cardinality();
			assert totalProducedMsgs <= para;
			if (totalProducedMsgs == para) {
				if (logCoord) LOG.info("checkForClosingProduced(" + bagID + ", " + s + ", opID = " + opID + "): produceClosed");
				s.produceClosed = true;
			}
		} else {

			// Az isEmpty produced-janak lezarasa vegett van most ez bent. Annak ugyebar 1 a para-ja, es onnan fog is kuldeni.
			// (Itt lehetne kicsit szebben, hogy ne legyen kodduplikalas a fentebbi resszel.)
			int totalProducedMsgs = s.producedSubtasks.cardinality();
			assert totalProducedMsgs <= para;
			if (totalProducedMsgs == para) {
				if (logCoord) LOG.info("checkForClosingProduced(" + bagID + ", " + s + ", opID = " + opID + "): produceClosed, because totalProducedMsgs == para");
				s.produceClosed = true;
			}

			if (!s.produceClosed) {
				boolean needMore = false;
				// Ebbe rakjuk ossze az inputok consumedSubtasks-jait
				BitSet needProduced = new BitSet(maxPara);
				for (BagID inp : s.inputs) {
					if (emptyBags.contains(inp)) {
						// enelkul olyankor lenne gond, ha egy binaris operator egyik inputja ures, emiatt a closeInputBag
						// mindegyik instance-t megloki, viszont a checkForClosingProduced csak a masik input alapjan nezi,
						// hogy honnan kell jonni, es ezert nem szamit bizonyos jovesekre
						for (int i = 0; i < para; i++) {
							needProduced.set(i);
						}
					}
					BagConsumptionStatus bcs = bagConsumedStatuses.get(new BagIDAndOpID(inp, opID));
					if (bcs != null) {
						if (!bcs.consumeClosed) {
							if (logCoord)
								LOG.info("checkForClosingProduced(" + bagID + ", " + s + ", opID = " + opID + "): needMore, because !bcs.consumeClosed");
							needMore = true;
							break;
						}
						//needProduced.addAll(bcs.consumedSubtasks);
						needProduced.or(bcs.consumedSubtasks);
					} else {
						// Maybe we run into trouble here if this happens because we have a binary operator that sends produced while it haven't yet consumed from one of its inputs.
						// Hm, but actually I don't think I have such an operator at the moment.   Ez kozben mar nem ervenyes
						// But this can actually happen with the nonEmpty stuff. But then we just close produced here.
						// Vagy esetleg az is lehet, hogy azert kerultunk ide mert nem a termeszetes sorrendben jott a produced es a consumed?
						if (logCoord)
							LOG.info("checkForClosingProduced(" + bagID + ", " + s + ", opID = " + opID + "): bcs == null");
					}
				}
				if (!needMore && !s.produceClosed) {
					int needed = needProduced.cardinality();
					int actual = s.producedSubtasks.cardinality();
					assert actual <= needed || needed == 0; // This should be true, because we have already checked consumeClose above. (needed == 0 when responding to empty input bags)
					if (actual < needed) {
						if (logCoord)
							LOG.info("checkForClosingProduced(" + bagID + ", " + s + ", opID = " + opID + "): needMore, because actual = " + actual + ", needed = " + needed);
						needMore = true;
					}
				}
				if (!needMore && !s.produceClosed) {
					if (logCoord)
						LOG.info("checkForClosingProduced(" + bagID + ", " + s + ", opID = " + opID + "): produceClosed");
					s.produceClosed = true;
				}
			}
		}

		if (s.produceClosed) {
			if (s.numProduced == 0) {
				if (logCoord) LOG.info("checkForClosingProduced(" + bagID + ", " + s + ", opID = " + opID + ") detected an empty bag");
				emptyBags.add(bagID);
				closeInputBagLocal(bagID, CloseInputBag.emptyBag);
			}

			if (barrier) {
				if (opsInLoop.contains(opID)) {
					Integer oldVal = barrierReached.get(bagID.cflSize);
					if (oldVal == null) {
						barrierReached.put(bagID.cflSize,1);
					} else {
						barrierReached.put(bagID.cflSize, oldVal + 1);
					}
					int newVal = barrierReached.get(bagID.cflSize);
					if (logCoord) LOG.info("checkForClosingProduced(" + bagID + ", " + s + ", opID = " + opID + ") barrierReached cflSize=" + bagID.cflSize + ", newVal=" + newVal + ", opsInLoop.size==" + opsInLoop.size());
					if (newVal == opsInLoop.size()) { // (a +1 amiatt kell, mert a MutableBag ketszer kell)
						barrierAllReachedLocal(bagID.cflSize);
					}
				}
			}
		}
	}

    // A coordinator a local itt. Az operatorok inputjainak a close-olasat valtja ez ki.
    private synchronized void closeInputBagLocal(BagID bagID, int opID) {
		assert coordinator;
		sendToEveryone(new Msg(jobCounter, new CloseInputBag(bagID, opID)));
    }

	// (runs on client)
    private synchronized void closeInputBagRemote(BagID bagID, int opID) {
		if (logCoord) LOG.info("closeInputBagRemote(" + bagID + ", " + opID +")");

		closeInputBags.add(new CloseInputBag(bagID, opID));

		ArrayList<CFLCallback> origCallbacks = new ArrayList<>(callbacks);
		for (CFLCallback cb: origCallbacks) {
			cb.notifyCloseInput(bagID, opID);
		}
    }


    private void subscribeCntLocal() {
		sendToEveryone(new Msg(jobCounter, new SubscribeCnt()));
	}

	private synchronized void subscribeCntRemote() {
		numSubscribed++;
		checkVoteStop();
	}

	private synchronized void barrierAllReachedLocal(int cflSize) {
		assert coordinator;
		sendToEveryone(new Msg(jobCounter, new BarrierAllReached(cflSize)));
	}

	private synchronized void barrierAllReachedRemote(int cflSize) {
		if (logCoord) LOG.info("barrierAllReachedRemote(" + cflSize + ")");

		ArrayList<CFLCallback> origCallbacks = new ArrayList<>(callbacks);
		for (CFLCallback cb: origCallbacks) {
			cb.notifyBarrierAllReached(cflSize);
		}
	}

	private synchronized void voteStopLocal() {
		LOG.info("voteStopLocal()");
		sendToCoordinator(new Msg(jobCounter, new VoteStop()));
	}

	private int numVoteStops = 0;

	private synchronized void voteStopRemote() {
		LOG.info("voteStopRemote()");
		assert coordinator;
    	numVoteStops++;
    	if (numVoteStops == hosts.length) {
    		stopLocal();
		}
	}

	private synchronized void stopLocal() {
		LOG.info("stopLocal()");
		assert coordinator;
		sendToEveryone(new Msg(jobCounter, new Stop()));
	}

	private synchronized void stopRemote() {
		LOG.info("stopRemote()");
		LOG.info("tm.CFLVoteStop();");
		tm.CFLVoteStop();
		setJobID(null);
		reset();
	}

    // --------------------------------

    public static final class Msg {

		public short jobCounter;

		// These are nullable, and exactly one should be non-null
		public CFLElement cflElement;
		public Consumed consumed;
		public Produced produced;
		public CloseInputBag closeInputBag;
		public SubscribeCnt subscribeCnt;
		public BarrierAllReached barrierAllReached;
		public VoteStop voteStop;
		public Stop stop;


		public void serialize(DataOutputView target) throws IOException {

			target.writeShort(jobCounter);

			if (cflElement != null) {
				target.writeByte(0);
				cflElement.serialize(target);
			} else if (consumed != null) {
				target.writeByte(1);
				consumed.serialize(target);
			} else if (produced != null) {
				target.writeByte(2);
				produced.serialize(target);
			} else if (closeInputBag != null) {
				target.writeByte(3);
				closeInputBag.serialize(target);
			} else if (subscribeCnt != null) {
				target.writeByte(4);
				subscribeCnt.serialize(target);
			} else if (barrierAllReached != null) {
				target.writeByte(5);
				barrierAllReached.serialize(target);
			} else if (voteStop != null) {
				target.writeByte(6);
				voteStop.serialize(target);
			} else if (stop != null) {
				target.writeByte(7);
				stop.serialize(target);
			}
		}

		/**
		 * All fields of r should be null!
		 */
		public static void deserialize(Msg r, DataInputView src) throws IOException {

			r.jobCounter = src.readShort();

			byte c = src.readByte();
			switch (c) {
				case 0:
					r.cflElement = new CFLElement();
					CFLElement.deserialize(r.cflElement, src);
					break;
				case 1:
					r.consumed = new Consumed();
					Consumed.deserialize(r.consumed, src);
					break;
				case 2:
					r.produced = new Produced();
					Produced.deserialize(r.produced, src);
					break;
				case 3:
					r.closeInputBag = new CloseInputBag();
					CloseInputBag.deserialize(r.closeInputBag, src);
					break;
				case 4:
					r.subscribeCnt = new SubscribeCnt();
					SubscribeCnt.deserialize(r.subscribeCnt, src);
					break;
				case 5:
					r.barrierAllReached = new BarrierAllReached();
					BarrierAllReached.deserialize(r.barrierAllReached, src);
					break;
				case 6:
					r.voteStop = new VoteStop();
					VoteStop.deserialize(r.voteStop, src);
					break;
				case 7:
					r.stop = new Stop();
					Stop.deserialize(r.stop, src);
					break;
			}
		}


		void assertOK() {
			int c = 0;

			if (cflElement != null) c++;
			if (consumed != null) c++;
			if (produced != null) c++;
			if (closeInputBag != null) c++;
			if (subscribeCnt != null) c++;
			if (barrierAllReached != null) c++;
			if (voteStop != null) c++;
			if (stop != null) c++;

			if (c != 1) {
				LOG.error("Corrupted msg: " + this.toString());
			}
			assert c == 1;
		}

		public Msg() {}

		public Msg(short jobCounter, CFLElement cflElement) {
			this.jobCounter = jobCounter;
			this.cflElement = cflElement;
		}

		public Msg(short jobCounter, Consumed consumed) {
			this.jobCounter = jobCounter;
			this.consumed = consumed;
		}

		public Msg(short jobCounter, Produced produced) {
			this.jobCounter = jobCounter;
			this.produced = produced;
		}

		public Msg(short jobCounter, CloseInputBag closeInputBag) {
			this.jobCounter = jobCounter;
			this.closeInputBag = closeInputBag;
		}

		public Msg(short jobCounter, SubscribeCnt subscribeCnt) {
			this.jobCounter = jobCounter;
			this.subscribeCnt = subscribeCnt;
		}

		public Msg(short jobCounter, BarrierAllReached barrierAllReached) {
			this.jobCounter = jobCounter;
			this.barrierAllReached = barrierAllReached;
		}

		public Msg(short jobCounter, VoteStop voteStop) {
			this.jobCounter = jobCounter;
			this.voteStop = voteStop;
		}

		public Msg(short jobCounter, Stop stop) {
			this.jobCounter = jobCounter;
			this.stop = stop;
		}

		@Override
		public String toString() {
			return "Msg{" +
					"jobCounter=" + jobCounter +
					", cflElement=" + cflElement +
					", consumed=" + consumed +
					", produced=" + produced +
					", closeInputBag=" + closeInputBag +
					", subscribeCnt=" + subscribeCnt +
					", barrierAllReached=" + barrierAllReached +
					", voteStop=" + voteStop +
					", stop=" + stop +
					'}';
		}
	}

	public static final class Consumed {

		public BagID bagID;
		public int numElements;
		public short subtaskIndex;
		public int opID;


		public void serialize(DataOutputView target) throws IOException {
			bagID.serialize(target);
			target.writeInt(numElements);
			target.writeShort(subtaskIndex);
			target.writeInt(opID);
		}

		public static void deserialize(Consumed r, DataInputView src) throws IOException {
			r.bagID = new BagID();
			BagID.deserialize(r.bagID, src);
			r.numElements = src.readInt();
			r.subtaskIndex = src.readShort();
			r.opID = src.readInt();
		}


		public Consumed() {}

		public Consumed(BagID bagID, int numElements, short subtaskIndex, int opID) {
			this.bagID = bagID;
			this.numElements = numElements;
			this.subtaskIndex = subtaskIndex;
			this.opID = opID;
		}

		@Override
		public String toString() {
			return "Consumed{" +
					"bagID=" + bagID +
					", numElements=" + numElements +
					", subtaskIndex=" + subtaskIndex +
					", opID=" + opID +
					'}';
		}
	}

	public static final class Produced {

		public BagID bagID;
		public BagID[] inpIDs;
		public int numElements;
		public short para;
		public short subtaskIndex;
		public int opID;


		public void serialize(DataOutputView target) throws IOException {
			bagID.serialize(target);
			assert inpIDs.length <= Byte.MAX_VALUE;
			target.writeByte(inpIDs.length);
			for (BagID bid: inpIDs) {
				bid.serialize(target);
			}
			target.writeInt(numElements);
			target.writeShort(para);
			target.writeShort(subtaskIndex);
			target.writeInt(opID);
		}

		public static void deserialize(Produced r, DataInputView src) throws IOException {

			r.bagID = new BagID();
			BagID.deserialize(r.bagID, src);

			byte l = src.readByte();
			r.inpIDs = new BagID[l];
			for (int i=0; i<l; i++) {
				r.inpIDs[i] = new BagID();
				BagID.deserialize(r.inpIDs[i], src);
			}

			r.numElements = src.readInt();
			r.para = src.readShort();
			r.subtaskIndex = src.readShort();
			r.opID = src.readInt();
		}


		public Produced() {}

		public Produced(BagID bagID, BagID[] inpIDs, int numElements, short para, short subtaskIndex, int opID) {
			this.bagID = bagID;
			this.inpIDs = inpIDs;
			this.numElements = numElements;
			this.para = para;
			this.subtaskIndex = subtaskIndex;
			this.opID = opID;
		}

		@Override
		public String toString() {
			return "Produced{" +
					"bagID=" + bagID +
					", inpIDs=" + Arrays.toString(inpIDs) +
					", numElements=" + numElements +
					", para=" + para +
					", subtaskIndex=" + subtaskIndex +
					", opID=" + opID +
					'}';
		}
	}

	public static class CloseInputBag {

		// Used in the opID field, when the bag is empty. In this case, all operators consuming this bag will close it.
		// They will also send produced(0), if they are marked with EmptyFromEmpty.
		public final static int emptyBag = -100;

		public BagID bagID;
		public int opID;


		public void serialize(DataOutputView target) throws IOException {
			bagID.serialize(target);
			target.writeInt(opID);
		}

		public static void deserialize(CloseInputBag r, DataInputView src) throws IOException {
			r.bagID = new BagID();
			BagID.deserialize(r.bagID, src);
			r.opID = src.readInt();
		}


		public CloseInputBag() {}

		public CloseInputBag(BagID bagID, int opID) {
			this.bagID = bagID;
			this.opID = opID;
		}

		@Override
		public String toString() {
			return "CloseInputBag{" +
					"bagID=" + bagID +
					", opID=" + opID +
					'}';
		}
	}

	public static class SubscribeCnt {
		public byte dummy; // To make it a POJO  (not serialized in the manual serializers!)

		public void serialize(DataOutputView target) throws IOException {
			// Nothing to do
		}

		public static void deserialize(SubscribeCnt r, DataInputView src) throws IOException {
			// Nothing to do
		}
	}

	public static class BarrierAllReached {

		public int cflSize;


		public void serialize(DataOutputView target) throws IOException {
			target.writeInt(cflSize);
		}

		public static void deserialize(BarrierAllReached r, DataInputView src) throws IOException {
			r.cflSize = src.readInt();
		}


		public BarrierAllReached() {}

		public BarrierAllReached(int cflSize) {
			this.cflSize = cflSize;
		}

		@Override
		public String toString() {
			return "BarrierAllReached{" +
					"cflSize=" + cflSize +
					'}';
		}
	}

	public static class VoteStop {
		public byte dummy; // To make it a POJO  (not serialized in the manual serializers!)

		public void serialize(DataOutputView target) throws IOException {
			// Nothing to do
		}

		public static void deserialize(VoteStop r, DataInputView src) throws IOException {
			// Nothing to do
		}
	}

	public static class Stop {
		public byte dummy; // To make it a POJO  (not serialized in the manual serializers!)

		public void serialize(DataOutputView target) throws IOException {
			// Nothing to do
		}

		public static void deserialize(Stop r, DataInputView src) throws IOException {
			// Nothing to do
		}
	}

	// --------------------------------
}
