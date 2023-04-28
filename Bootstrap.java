//import org.jetbrains.annotations.NotNull;

import javax.xml.crypto.Data;
import java.io.*;
import java.net.Inet4Address;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

public class Bootstrap {
    ArrayList<Integer> serverIDS = new ArrayList<>();
    HashMap<Integer, String> data = new HashMap<>();
    static HashMap<Integer, String> nsConnAll = new HashMap<>();
    static ServerSocket serverSocket;
    static Socket socket;
    static int ID;
    static int serverPort;
    NSOperations nsOperations;
//    System.out.println(data + " " + ID + " " + serverPort);
    public Bootstrap() throws IOException {
        this.serverIDS.add(ID);
        this.nsOperations = new NSOperations(data, ID, serverPort, serverIDS);
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException {
        File confFile = new File(args[0]);
//        read config from file
        Scanner scanner = new Scanner(confFile);
        ID = Integer.parseInt(scanner.nextLine());
        serverPort = Integer.parseInt(scanner.nextLine());

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.nsOperations.nsMeta.setRingMeta(ID, Inet4Address.getLocalHost().getHostAddress(), serverPort, ID, Inet4Address.getLocalHost().getHostAddress(), serverPort);
//        serverSocket = new ServerSocket(serverPort);
        while(scanner.hasNextLine()) {
            String[] kv = scanner.nextLine().split(" ");
            bootstrap.nsOperations.data.put(Integer.parseInt(kv[0]), kv[1]);
        }
        nsConnAll.put(ID, bootstrap.nsOperations.nsMeta.getIP() + ":" + bootstrap.nsOperations.nsMeta.getServerPort());
//        bootstrap.nsOperations.printInfo();
        new Thread(new BootstrapUI(bootstrap)).start();

        //
//        Socket nsSockConn = new Socket("172.19.49.101", 4578);
//        new DataOutputStream(nsSockConn.getOutputStream()).writeUTF("sendKV " + 0 + " to " + 100);
        while(true) {
            serverSocket = new ServerSocket(serverPort);
            socket = serverSocket.accept();

            System.out.println("Connection accepted");

            DataInputStream dis = new DataInputStream(socket.getInputStream());
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            String nsMsg = dis.readUTF();
            System.out.println(nsMsg);
            if(nsMsg.trim().split(" ")[0].equals("enter"))
                bootstrap.nsEntering(nsMsg); //syntax: `enter <ID> <IP> <port>`
            else if (nsMsg.trim().split(" ")[0].equals("updatePredecessor"))
                bootstrap.updatePredecessor(nsMsg);
            else if (nsMsg.trim().split(" ")[0].equals("updateSuccessor"))
                bootstrap.updateSuccessor(nsMsg);
            else if (nsMsg.trim().split(" ")[0].equals("exit"))
                bootstrap.nsExiting(nsMsg);
            System.out.print("\nbootstrapSh$> ");
            socket.close();
            serverSocket.close();
            Socket nsSockConn = new Socket("172.19.49.101", 4578);
            new DataOutputStream(nsSockConn.getOutputStream()).writeUTF("sendKV " + 0 + " to " + 100);
        }
    }
    private void nsEntering(String nsMsg) throws IOException {
        int nsID = Integer.parseInt(nsMsg.trim().split(" ")[1]);
        String nsIP = nsMsg.trim().split(" ")[2];
        int nsPort = Integer.parseInt(nsMsg.trim().split(" ")[3]);
        int tempPort = Integer.parseInt(nsMsg.trim().split(" ")[4]);
        Socket nsSockConn = new Socket(nsIP, nsPort);
        Socket tempNS = new Socket(nsIP, tempPort);
        DataOutputStream tempdos = new DataOutputStream(tempNS.getOutputStream());

        nsConnAll.put(nsID, nsIP+":"+nsPort);
//        DataOutputStream dos = new DataOutputStream(nsSockConn.getOutputStream());
        if(this.serverIDS.contains(nsID)) {
            tempdos.writeUTF("Error: Duplicate NameServer ID");
            System.out.println("Error: Duplicate NameServer ID");
            return;
        }
        serverIDS.add(nsID);
        int cursPred = 0;
        int cursSucc = 0;
        Collections.sort(serverIDS);
        for(int id : serverIDS) {
            if(id < nsID) cursPred = id;
            else if (id > nsID) { cursSucc = id; break; }
        }
        List<Integer> deleteKey =  getKeysFromSuccessor(nsID, cursPred, cursSucc, tempdos);
        System.out.println("data transferred");
//            System.out.println(serverIDS);
            System.out.println(nsConnAll);
//          Here check special case for Bootstrap server
        if(this.nsOperations.nsMeta.getPredecessorID() == 0 && this.nsOperations.nsMeta.getSuccessorID() == 0){
//              only 1st name server entering will have succ and pred as Bootstrap
            this.nsOperations.nsMeta.updatePredecessor(nsID, nsIP, nsPort);
            this.nsOperations.nsMeta.updateSuccessor(nsID, nsIP, nsPort);
            tempdos.writeUTF("updatePredecessor: " + this.nsOperations.nsMeta.getID() + " " + this.nsOperations.nsMeta.getIP() + " " + this.nsOperations.nsMeta.getServerPort());
            tempdos.writeUTF("updateSuccessor: " + this.nsOperations.nsMeta.getID() + " " + this.nsOperations.nsMeta.getIP() + " " + this.nsOperations.nsMeta.getServerPort());
        } else {
//              now get cursPred's Socket connection and say to update their succ and get their details
            if(cursPred == 0){
                System.out.println("updating self's successor");
                this.nsOperations.nsMeta.updateSuccessor(nsID, nsIP, nsPort);
                tempdos.writeUTF("updatePredecessor: " + this.nsOperations.nsMeta.getID() + " " + this.nsOperations.nsMeta.getIP() + " " + this.nsOperations.nsMeta.getServerPort());
            } else {
                Socket predSock = new Socket(nsConnAll.get(cursPred).split(":")[0], Integer.parseInt(nsConnAll.get(cursPred).split(":")[1]));
                System.out.println("updating predecessor's successor");
                tempdos.writeUTF("updatePredecessor: " + cursPred + " " + predSock.getInetAddress() + " " + predSock.getPort());
                new DataOutputStream(predSock.getOutputStream()).writeUTF("updateSuccessor: " + nsID + " " + nsIP + " " + nsPort);
            }
//              then get cursSucc's Socket connection and say to update their pred
            if(cursSucc == 0){
                System.out.println("updating self's predecessor");
                this.nsOperations.nsMeta.updatePredecessor(nsID, nsIP, nsPort);
                tempdos.writeUTF("updateSuccessor: " + this.nsOperations.nsMeta.getID() + " " + this.nsOperations.nsMeta.getIP() + " " + this.nsOperations.nsMeta.getServerPort());
            } else {
                System.out.println("updating successor's predecessor");
                Socket succSock = new Socket(nsConnAll.get(cursSucc).split(":")[0], Integer.parseInt(nsConnAll.get(cursSucc).split(":")[1]));
                tempdos.writeUTF("updateSuccessor: " + cursSucc + " " + succSock.getInetAddress() + " " + succSock.getPort());
                new DataOutputStream(succSock.getOutputStream()).writeUTF("updatePredecessor: " + nsID + " " + nsIP + " " + nsPort);
            }
//                now transfer the keys to the newly added NameServer
        }
//            delete the transfered key after completing pred and succ update
        for(Integer k : deleteKey) {
            this.data.remove(k);
        }

        Collections.sort(serverIDS);
        System.out.println(serverIDS);
//        System.out.println(this.nsOperations.nsMeta.getPredecessorID());
//        System.out.println(this.nsOperations.nsMeta.getSuccessorID());
        System.out.println("NameServer " + nsID + " successfully entered the system.");
        nsOperations.printInfo();
        tempNS.close();
    }

    private List<Integer> getKeysFromSuccessor(int selfID, int predID, int succID, DataOutputStream dos) throws IOException {

        Socket succSock = new Socket(nsConnAll.get(succID).split(":")[0], Integer.parseInt(nsConnAll.get(succID).split(":")[1]));
        System.out.println(succSock);
        DataOutputStream succDos = new DataOutputStream(succSock.getOutputStream());
        DataInputStream succDis = new DataInputStream(succSock.getInputStream());
        System.out.println(selfID + " " + predID + " " + succID);
        List<Integer> deleteKey = new ArrayList<>();
        if(succID == 0) {
            System.out.println("transfering keys to new predecessor from " + predID+1 + " to " + selfID);
            for(int i = predID; i<selfID; i++) {
                if(data.containsKey(i)) {
                    dos.writeUTF(String.valueOf(i) + " " + data.get(i));
//                    data.remove(i);
                    deleteKey.add(i);
                }
            }
            dos.writeUTF("endTransfer");
        } else {
            System.out.println("getting data from " + succID );
//            System.out.println(nsConnAll);
            succDos.writeUTF("sendKV " + predID + " to " + selfID); // tranfer from predID to selfID(exclusive)
            while(true) {
//                System.out.println("\nwaiting");
                String resp = succDis.readUTF();
                if(!resp.equals("endTransfer")) {
                    dos.writeUTF(resp); //response should be of format 'key value'
                    deleteKey.add(Integer.valueOf(resp.split(" ")[0]));
                } else break;
            }
            dos.writeUTF("endTransfer");
        }
//        selfDos.writeUTF("KV transfer end");
        return deleteKey;
    }

    private void updatePredecessor(String nsMsg) {
    }
    private void updateSuccessor(String nsMsg) {

    }
    private void nsExiting(String nsMsg) throws IOException, UnknownHostException {
        int nsID = Integer.parseInt(nsMsg.trim().split(" ")[1]);
        String nsIP = nsMsg.trim().split(" ")[2];
        int nsPort = Integer.parseInt(nsMsg.trim().split(" ")[3]);
        int cursPred = 0;
        int cursSucc = 0;
        Collections.sort(serverIDS);
        for(int id : serverIDS) {
            if(id < nsID) cursPred = id;
            else if (id > nsID) { cursSucc = id; break; }
        }
        Socket nsSockConn = new Socket(nsConnAll.get(nsID).split(":")[0], Integer.parseInt(nsConnAll.get(nsID).split(":")[1]));
        DataOutputStream dos = new DataOutputStream(nsSockConn.getOutputStream());
        DataInputStream dis = new DataInputStream(nsSockConn.getInputStream());
        transferKeysToSuccessor(nsID, cursPred, cursSucc, dis, dos);
        if(cursPred == 0 && cursSucc == 0) {
            this.nsOperations.nsMeta.updatePredecessor(nsOperations.nsMeta.getID(), nsOperations.nsMeta.getIP(), nsOperations.nsMeta.getServerPort());
            this.nsOperations.nsMeta.updateSuccessor(nsOperations.nsMeta.getID(), nsOperations.nsMeta.getIP(), nsOperations.nsMeta.getServerPort());
        } else if(cursPred == 0 && cursSucc != 0) {
//          when leaving server is not linked to Bootstrap as successor
            Socket succSockConn = new Socket(nsConnAll.get(cursSucc).split(":")[0], Integer.parseInt(nsConnAll.get(cursSucc).split(":")[1]));
            this.nsOperations.nsMeta.updateSuccessor(cursSucc, String.valueOf(succSockConn.getInetAddress()), succSockConn.getPort());
            new DataOutputStream(succSockConn.getOutputStream()).writeUTF("predecessorInfo: " + nsOperations.nsMeta.getID() + " " + nsOperations.nsMeta.getIP() + " " + nsOperations.nsMeta.getServerPort());
        } else if(cursPred!=0 && cursSucc == 0) {
//          when leaving server is not linked to Bootstrap as predecessor
            Socket predSockConn = new Socket(nsConnAll.get(cursPred).split(":")[0], Integer.parseInt(nsConnAll.get(cursPred).split(":")[1]));
            new DataOutputStream(predSockConn.getOutputStream()).writeUTF("successorInfo: " + nsOperations.nsMeta.getID() + " " + nsOperations.nsMeta.getIP() + " " + nsOperations.nsMeta.getServerPort());
            this.nsOperations.nsMeta.updatePredecessor(cursPred, String.valueOf(predSockConn.getInetAddress()), predSockConn.getPort());
        } else {
//          when leaving server is not linked to Bootstrap as predecessor or successor
            Socket succSockConn = new Socket(nsConnAll.get(cursSucc).split(":")[0], Integer.parseInt(nsConnAll.get(cursSucc).split(":")[1]));
            Socket predSockConn = new Socket(nsConnAll.get(cursPred).split(":")[0], Integer.parseInt(nsConnAll.get(cursPred).split(":")[1]));
            new DataOutputStream(succSockConn.getOutputStream()).writeUTF("predecessorInfo: " + cursPred + " " + predSockConn.getInetAddress() + " " + predSockConn.getPort());
            new DataOutputStream(predSockConn.getOutputStream()).writeUTF("successorInfo: " + cursSucc + " " + succSockConn.getInetAddress() + " " + succSockConn.getPort());
        }
    }

    private void transferKeysToSuccessor(int selfID, int predID, int succID, DataInputStream dis, DataOutputStream dos) throws IOException {
        String resp;
        dos.writeUTF("send KV from " + predID + " to " + selfID + "-1"); // tranfer from predID to selfID(exclusive)

        if(succID != 0) {
            Socket succSocket = new Socket(nsConnAll.get(succID).split(":")[0], Integer.parseInt(nsConnAll.get(succID).split(":")[1]));
            DataOutputStream succDos = new DataOutputStream(succSocket.getOutputStream());
            while(true) {
                 resp = dis.readUTF();
                 if(!resp.equals("endTransfer")) {
                     succDos.writeUTF(resp); //resp format "Key Value"
                 } else break;
            }
        } else {
            while(true) {
                resp = dis.readUTF();
                if(!resp.equals("endTransfer")) {
                    this.nsOperations.data.put(Integer.valueOf(resp.split(" ")[0]), resp.split(" ")[0]); //resp format "Key Value"
                } else break;
            }
        }
    }
}

class BootstrapUI implements Runnable{
    Bootstrap bootstrap;
    public  BootstrapUI(Bootstrap bootstrap) {
        this.bootstrap = bootstrap;
    }

    @Override
    public void run() {
        String cmd = "";
        Scanner userInput = new Scanner(System.in);
        while(true) {
            System.out.print("bootstrapSh$> ");
            cmd = userInput.nextLine();
            if(Integer.parseInt(cmd.trim().split(" ")[1]) < 0 || Integer.parseInt(cmd.trim().split(" ")[1]) > 1023) {
                System.out.println("Error: Key " + cmd.trim().split(" ")[1] + " is out or range");
                continue;
            }
            System.out.println(cmd);
            if(cmd.trim().split(" ")[0].equals("lookup")) {
                try {
                    String value = bootstrap.nsOperations.lookup(Integer.parseInt(cmd.trim().split(" ")[1]), ""); // hopstart at bootstrap server with 0 as ID
                    System.out.println("Key: " + cmd.trim().split(" ")[1] + " Value: " + value.split(" ")[0]);
                    String hopInfo = value.split(" ")[1].substring(0, value.split(" ")[1].length() - 1);
                    System.out.println("Nodes searched : " + value.split(" ")[1]);
                } catch (IOException | ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            } else if (cmd.trim().split(" ")[0].equals("insert")) {
                try {
                    String response = bootstrap.nsOperations.insert(Integer.parseInt(cmd.trim().split(" ")[1]), cmd.trim().split(" ")[2], ""); // hopstart at bootstrap server with 0 as ID
                    String hopInfo = response.split(" ")[0].substring(0, response.split(" ")[0].length() - 1);
                    System.out.println("Nodes searched : " + hopInfo);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else if (cmd.trim().split(" ")[0].equals("delete")) {
                try {
                    String response = bootstrap.nsOperations.delete(Integer.parseInt(cmd.trim().split(" ")[1]), ""); // hopstart at bootstrap server with 0 as ID
                    String hopInfo = response.split(" ")[1].substring(0, response.split(" ")[1].length() - 1);
                    System.out.println("Nodes searched : " + hopInfo);
                    System.out.println(response.split(" ")[0]);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                System.out.println("Error: command not found..");
            }
        }

    }
}
