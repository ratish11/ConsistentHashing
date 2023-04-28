import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.TreeMap;

public class NSMeta {
	private int ID, predecessorID, successorID, predecessorPort, successorPort, serverPort;
	private String predecessorIP, successorIP;
	HashMap<Integer, String> data;
	public NSMeta(int ID, int serverPort, HashMap<Integer, String> data) {
		this.ID = ID;
		this.serverPort = serverPort;
		predecessorIP = null;
		successorIP = null;
		predecessorID = 0;
		successorID = 0;
		predecessorPort = 0;
		successorPort = 0;
		this.data = data;
	}
	public void setRingMeta(int predecessorID, String predecessorIP, int predecessorPort, int successorID, String successorIP, int successorPort) {
		this.predecessorID = predecessorID;
		this.predecessorIP = predecessorIP;
		this.predecessorPort = predecessorPort;
		this.successorID = successorID;
		this.successorIP = successorIP;
		this.successorPort = successorPort;
	}

	public int getID() { return ID; }
	public int getServerPort() { return serverPort; }
	public String getIP() throws UnknownHostException {return Inet4Address.getLocalHost().getHostAddress();}

	public void updatePredecessor(int predecessorID, String predecessorIP, int predecessorPort) {
		this.predecessorID = predecessorID;
		this.predecessorIP = predecessorIP;
		this.predecessorPort = predecessorPort;
	}
	public void updateSuccessor(int successorID, String successorIP, int successorPort) {
		this.successorID = successorID;
		this.successorIP = successorIP;
		this.successorPort = successorPort;
	}
	public int getPredecessorID() {
		return this.predecessorID;
	}
	public void putPredecessorID(int predecessorID) {
		this.predecessorID = predecessorID;
	}
	public String getPredecessorIP() {
		return this.predecessorIP;
	}
	public void putPredecessorIP(String predecessorIP) {
		this.predecessorIP = predecessorIP;
	}
	public  int getPredecessorPort() {
		return this.predecessorPort;
	}
	public  void putPredecessorPort(int predecessorPort) {
		this.predecessorPort = predecessorPort;
	}
	public int getSuccessorID() {
		return this.successorID;
	}
	public String getSuccessorIP() {
		return this.successorIP;
	}
	public  int getSuccessorPort() {
		return this.successorPort;
	}
	public void printInfo() {
		TreeMap sorted = new TreeMap<>();
		sorted.putAll(data);
		System.out.println(sorted);
		System.out.println("Predecessor: " + getPredecessorID());
		System.out.println("Successor: " + getSuccessorID());
	}
}