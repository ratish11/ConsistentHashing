import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class NSOperations {
    HashMap<Integer, String> data;
    int ID, serverPort;
    NSMeta nsMeta = null;
    static Socket nxtSrvSocket = null;
    DataInputStream dis = null;
    DataOutputStream dos = null;
    public NSOperations(HashMap<Integer, String> data, int ID, int serverPort) {
        this.data = data;
        this.ID = ID;
        this.serverPort = serverPort;
        this.nsMeta = new NSMeta(this.ID, this.serverPort);
    }
    public String lookup(int key, String hopInfo) throws UnknownHostException, IOException, ClassNotFoundException {
        String value = null;
//        if(this.ID == 0) {
//            hopInfo = "";
//        }
        hopInfo += String.valueOf(this.ID) + "-";
        if(data.containsKey(key)) {
            System.out.println("Nodes searched :" + hopInfo.substring(0, hopInfo.length() - 1));
            value = data.get(key);
            return value;
        } else {
            nxtSrvSocket = new Socket(nsMeta.getSuccessorIP(), nsMeta.getSuccessorPort());
            dis = new DataInputStream(nxtSrvSocket.getInputStream());
            dos = new DataOutputStream(nxtSrvSocket.getOutputStream());
            dos.writeUTF("lookup " + String.valueOf(key));
            dos.writeUTF(hopInfo);
            value = dis.readUTF();
            hopInfo = dis.readUTF();
        }
        return value;
    }

    public boolean insert(int key, String value, String hopInfo, ArrayList<Integer> serverIDS) throws IOException {
        hopInfo += this.ID + "-";
        if(key > Collections.max(serverIDS)) {
            System.out.println("Nodes searched :" + hopInfo.substring(0, hopInfo.length() - 1));
            data.put(key, value);
            return true;
        } else if(key <= nsMeta.getSuccessorID()){
            nxtSrvSocket = new Socket(nsMeta.getSuccessorIP(), nsMeta.getSuccessorPort());
            dis = new DataInputStream(nxtSrvSocket.getInputStream());
            dos = new DataOutputStream(nxtSrvSocket.getOutputStream());
            dos.writeUTF("insert " + String.valueOf(key) + " " + value);
            dos.writeUTF(hopInfo);
            hopInfo = dis.readUTF();
            return true;
        }
        System.out.println("Nodes searched :" + hopInfo.substring(0, hopInfo.length() - 1));
        System.out.println("Key: " + key + " not found");
        return false;
    }

    public boolean delete(int key, String hopInfo, ArrayList<Integer> serverIDS) throws IOException {
        hopInfo += this.ID + "-";
        if(key > Collections.max(serverIDS)) {
            System.out.println("Nodes searched :" + hopInfo.substring(0, hopInfo.length() - 1));
            data.remove(key);
            return true;
        } else if (key <= nsMeta.getSuccessorID()) {
            nxtSrvSocket = new Socket(nsMeta.getSuccessorIP(), nsMeta.getSuccessorPort());
            dis = new DataInputStream(nxtSrvSocket.getInputStream());
            dos = new DataOutputStream(nxtSrvSocket.getOutputStream());
            dos.writeUTF("delete " + String.valueOf(key));
            dos.writeUTF(hopInfo);
            hopInfo = dis.readUTF();
            return true;
        }
        System.out.println("Nodes searched :" + hopInfo.substring(0, hopInfo.length() - 1));
        System.out.println("Key: " + key + " not found");
        return false;
    }
}

class NSMeta {
    private int ID, predecessorID, successorID, predecessorPort, successorPort, serverPort;
    private String predecessorIP, successorIP;

    public NSMeta(int ID, int serverPort) {
        this.ID = ID;
        this.serverPort = serverPort;
        predecessorIP = null;
        successorIP = null;
        predecessorID = Integer.parseInt(null);
        successorID = Integer.parseInt(null);
        predecessorPort = Integer.parseInt(null);
        successorPort = Integer.parseInt(null);
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
    public String getPredecessorIP() {
        return this.predecessorIP;
    }
    public  int getPredecessorPort() {
        return this.predecessorPort;
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
}