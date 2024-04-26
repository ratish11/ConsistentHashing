import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.TreeMap;

public class NSOperations {
    HashMap<Integer, String> data;
    int ID, serverPort;
    ArrayList<Integer> serverIDS;
    NSMeta nsMeta = null;
    static Socket nxtSrvSocket = null;
    DataInputStream dis = null;
    DataOutputStream dos = null;
    public NSOperations(HashMap<Integer, String> data, int ID, int serverPort, ArrayList<Integer> serverIDS) {
        this.data = data;
        this.ID = ID;
        this.serverPort = serverPort;
        this.serverIDS = serverIDS;
        this.nsMeta = new NSMeta(ID, serverPort);
//        System.out.println(this.nsMeta);
    }
    public NSOperations(HashMap<Integer, String> data, int ID, int serverPort) {
        this.data = data;
        this.ID = ID;
        this.serverPort = serverPort;
        this.nsMeta = new NSMeta(ID, serverPort);
//        System.out.println(this.nsMeta);
    }
    public String lookup(int key, String hopInfo) throws UnknownHostException, IOException, ClassNotFoundException {
        String value = null;
        hopInfo += this.ID + "-";
        if(key>=nsMeta.getPredecessorID()) {
            if(this.data.containsKey(key)) {
//                System.out.println("Nodes searched :" + hopInfo.substring(0, hopInfo.length() - 1));
                value = this.data.get(key);
//                System.out.println("Key: " + key + " Value: " + value);
            } else {
                value = "NotFound";
                System.out.println("Error: Key " + key + " not found.");
            }
        } else {
            nxtSrvSocket = new Socket(nsMeta.getSuccessorIP(), nsMeta.getSuccessorPort());
            dis = new DataInputStream(nxtSrvSocket.getInputStream());
            dos = new DataOutputStream(nxtSrvSocket.getOutputStream());
            dos.writeUTF("lookup " + String.valueOf(key));
            dos.writeUTF(hopInfo);
            value = dis.readUTF();
            hopInfo = dis.readUTF();
        }
        return value + " " + hopInfo;
    }

    public String insert(int key, String value, String hopInfo) throws IOException {
        hopInfo += this.ID + "-";
        if (key>=nsMeta.getPredecessorID()){
            System.out.println("Nodes searched :" + hopInfo.substring(0, hopInfo.length() - 1));
            data.put(key, value);
            printInfo();
            System.out.println("Key Value Pair Inserted at server: " + this.ID);
            printInfo();
        } else {
            nxtSrvSocket = new Socket(nsMeta.getSuccessorIP(), nsMeta.getSuccessorPort());
            dis = new DataInputStream(nxtSrvSocket.getInputStream());
            dos = new DataOutputStream(nxtSrvSocket.getOutputStream());
            dos.writeUTF("insert " + String.valueOf(key) + " " + value);
            dos.writeUTF(hopInfo);
            hopInfo = dis.readUTF();
        }
        return hopInfo;
    }

    public String delete(int key, String hopInfo) throws IOException {
        hopInfo += this.ID + "-";
        String response;
        if(key>=nsMeta.getPredecessorID()) {
            if(data.containsKey(key)) {
//                System.out.println("Nodes searched :" + hopInfo.substring(0, hopInfo.length() - 1));
                data.remove(key);
                response = "keyDeleted";
                printInfo();
            } else {
                response = "NotFound";
                System.out.println(response);
            }
        } else {
            System.out.println("calling successor " + nsMeta.getPredecessorID());
            Socket nxtSrvSocket = new Socket(nsMeta.getSuccessorIP(), nsMeta.getSuccessorPort());
            dis = new DataInputStream(nxtSrvSocket.getInputStream());
            dos = new DataOutputStream(nxtSrvSocket.getOutputStream());
            dos.writeUTF("delete " + String.valueOf(key));
            dos.writeUTF(hopInfo);
            response = dis.readUTF();
            hopInfo = dis.readUTF();
        }
        return response + " " + hopInfo;
    }
    public void printInfo() {
        TreeMap sorted = new TreeMap<>();
        sorted.putAll(data);
        System.out.println(sorted);
        System.out.println(this.nsMeta.getPredecessorID());
        System.out.println(this.nsMeta.getSuccessorID());
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
        predecessorID = 0;
        successorID = 0;
        predecessorPort = 0;
        successorPort = 0;
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