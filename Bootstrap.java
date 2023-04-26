//import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.net.Inet4Address;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

public class Bootstrap {
    ArrayList<Integer> serverIDS = new ArrayList<>();
    HashMap<Integer, String> data = new HashMap<>();
    HashMap<Integer, Socket> nsConnAll = new HashMap<>();
    static ServerSocket serverSocket;
    static Socket socket;
    static int ID;
    static int serverPort;
    NSOperations nsOperations;
//    System.out.println(data + " " + ID + " " + serverPort);
    public Bootstrap() {
        this.serverIDS.add(ID);
        this.nsOperations = new NSOperations(data, ID, serverPort, serverIDS);
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException {
        File confFile = new File(args[0]);
//        read config from file
        Scanner scanner = new Scanner(confFile);
        ID = Integer.parseInt(scanner.nextLine());
        serverPort = Integer.parseInt(scanner.nextLine());
        serverSocket = new ServerSocket(serverPort);
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.nsOperations.nsMeta.setRingMeta(ID, Inet4Address.getLocalHost().getHostAddress(), serverPort, ID, Inet4Address.getLocalHost().getHostAddress(), serverPort);

        while(scanner.hasNextLine()) {
            String[] kv = scanner.nextLine().split(" ");
            bootstrap.nsOperations.data.put(Integer.parseInt(kv[0]), kv[1]);
        }
//        bootstrap.nsOperations.printInfo();
        new Thread(new BootstrapUI(bootstrap)).start();
        while(true) {
            socket = serverSocket.accept();
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
        }
    }
    private void nsEntering(String nsMsg) throws IOException {
        int nsID = Integer.parseInt(nsMsg.trim().split(" ")[1]);
        String nsIP = nsMsg.trim().split(" ")[2];
        int nsPort = Integer.parseInt(nsMsg.trim().split(" ")[3]);
        int cursPred = 0;
        int cursSucc = 0;
        try {
            Socket nsSockConn = new Socket(nsIP, nsPort);
            DataOutputStream dos = new DataOutputStream(nsSockConn.getOutputStream());
            DataInputStream dis = new DataInputStream(nsSockConn.getInputStream());
            serverIDS.add(nsID);
            nsConnAll.put(nsID, nsSockConn);
            List<Integer> deleteKey =  getKeysFromSuccessor(nsID, cursPred, cursSucc);
//            System.out.println(serverIDS);
//            System.out.println(nsConnAll);
//          Here check special case for Bootstrap server
            if(this.nsOperations.nsMeta.getPredecessorID() == 0 && this.nsOperations.nsMeta.getSuccessorID() == 0){
//              only 1st name server entering will have succ and pred as Bootstrap
                this.nsOperations.nsMeta.updatePredecessor(this.nsOperations.nsMeta.getID(), this.nsOperations.nsMeta.getIP(), this.nsOperations.nsMeta.getServerPort());
                this.nsOperations.nsMeta.updateSuccessor(this.nsOperations.nsMeta.getID(), this.nsOperations.nsMeta.getIP(), this.nsOperations.nsMeta.getServerPort());
                dos.writeUTF("predecessorInfo: " + this.nsOperations.nsMeta.getID() + " " + this.nsOperations.nsMeta.getIP() + " " + this.nsOperations.nsMeta.getServerPort());
                dos.writeUTF("successorInfo: " + this.nsOperations.nsMeta.getID() + " " + this.nsOperations.nsMeta.getIP() + " " + this.nsOperations.nsMeta.getServerPort());
            } else {
                Collections.sort(serverIDS);
                for(int id : serverIDS) {
                    if(id < nsID) cursPred = id;
                    else if (id > nsID) { cursSucc = id; break; }
                }
//              now get cursPred's Socket connection and say to update their succ and get their details
                if(cursPred == 0){
                    System.out.println("updating self's successor");
                    this.nsOperations.nsMeta.updateSuccessor(nsID, nsIP, nsPort);
                    dos.writeUTF("predecessorInfo: " + this.nsOperations.nsMeta.getID() + " " + this.nsOperations.nsMeta.getIP() + " " + this.nsOperations.nsMeta.getServerPort());
                } else {
                    Socket predSock = nsConnAll.get(cursPred);
                    System.out.println("updating predecessor's successor");
                    dos.writeUTF("predecessorInfo: " + cursPred + " " + predSock.getInetAddress() + " " + predSock.getPort());
                    new DataOutputStream(predSock.getOutputStream()).writeUTF("successorInfo: " + nsID + " " + nsIP + " " + nsPort);
                }
//              then get cursSucc's Socket connection and say to update their pred
                if(cursSucc == 0){
                    System.out.println("updating self's predecessor");
                    this.nsOperations.nsMeta.updatePredecessor(nsID, nsIP, nsPort);
                    dos.writeUTF("successorInfo: " + this.nsOperations.nsMeta.getID() + " " + this.nsOperations.nsMeta.getIP() + " " + this.nsOperations.nsMeta.getServerPort());
                } else {
                    System.out.println("updating successor's predecessor");
                    Socket succSock = nsConnAll.get(cursSucc);
                    dos.writeUTF("successorInfo: " + cursSucc + " " + succSock.getInetAddress() + " " + succSock.getPort());
                    new DataOutputStream(succSock.getOutputStream()).writeUTF("predecessorInfo: " + nsID + " " + nsIP + " " + nsPort);
                }
//                now transfer the keys to the newly added NameServer
            }
//            delete the transfered key after completing pred and succ update
            for(Integer k : deleteKey) {
                this.data.remove(k);
            }
//            nsOperations.printInfo();
        } catch (IOException io) {
            io.printStackTrace();
        }
        System.out.println(serverIDS);
        System.out.println(this.nsOperations.nsMeta.getPredecessorID());
        System.out.println(this.nsOperations.nsMeta.getSuccessorID());

        System.out.println("NameServer " + nsID + " successfully entered the system.");
    }

    private List<Integer> getKeysFromSuccessor(int selfID, int predID, int succID) throws IOException {
        DataOutputStream selfDos = new DataOutputStream(this.nsConnAll.get(selfID).getOutputStream());
//        selfDos.writeUTF("start KV transfer");
        List<Integer> deleteKey = new ArrayList<>();
        if(succID == 0) {
            for(int i = predID+1; i<=selfID; i++) {
                if(data.containsKey(i)) {
                    selfDos.writeUTF(String.valueOf(i) + " " + data.get(i));
//                    data.remove(i);
                    deleteKey.add(i);
                }
                selfDos.writeUTF("end transfer");
            }
        } else {
            DataOutputStream succDos = new DataOutputStream(nsConnAll.get(succID).getOutputStream());
            DataInputStream succDis = new DataInputStream(nsConnAll.get(succID).getInputStream());
            succDos.writeUTF("send KV from " + predID + " to " + selfID + "-1"); // tranfer from predID to selfID(exclusive)
            while(true) {
                String resp = succDis.readUTF();
                if(!resp.equals("end transfer")) {
                    selfDos.writeUTF(resp); //response should be of format 'key value'
                    deleteKey.add(Integer.valueOf(resp.split(" ")[0]));
                } else break;
            }
            selfDos.writeUTF("end transfer");
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
        Socket nsSockConn = nsConnAll.get(nsID);
        transferKeysToSuccessor(nsID, cursPred, cursSucc);
        if(cursPred == 0 && cursSucc == 0) {
            this.nsOperations.nsMeta.updatePredecessor(nsOperations.nsMeta.getID(), nsOperations.nsMeta.getIP(), nsOperations.nsMeta.getServerPort());
            this.nsOperations.nsMeta.updateSuccessor(nsOperations.nsMeta.getID(), nsOperations.nsMeta.getIP(), nsOperations.nsMeta.getServerPort());
        } else if(cursPred == 0 && cursSucc != 0) {
//          when leaving server is not linked to Bootstrap as successor
            Socket succSockConn = nsConnAll.get(cursSucc);
            this.nsOperations.nsMeta.updateSuccessor(cursSucc, String.valueOf(succSockConn.getInetAddress()), succSockConn.getPort());
            new DataOutputStream(succSockConn.getOutputStream()).writeUTF("predecessorInfo: " + nsOperations.nsMeta.getID() + " " + nsOperations.nsMeta.getIP() + " " + nsOperations.nsMeta.getServerPort());
        } else if(cursPred!=0 && cursSucc == 0) {
//          when leaving server is not linked to Bootstrap as predecessor
            Socket predSockConn = nsConnAll.get(cursPred);
            new DataOutputStream(predSockConn.getOutputStream()).writeUTF("successorInfo: " + nsOperations.nsMeta.getID() + " " + nsOperations.nsMeta.getIP() + " " + nsOperations.nsMeta.getServerPort());
            this.nsOperations.nsMeta.updatePredecessor(cursPred, String.valueOf(predSockConn.getInetAddress()), predSockConn.getPort());
        } else {
//          when leaving server is not linked to Bootstrap as predecessor or successor
            Socket succSockConn = nsConnAll.get(cursSucc);
            Socket predSockConn = nsConnAll.get(cursPred);
            new DataOutputStream(succSockConn.getOutputStream()).writeUTF("predecessorInfo: " + cursPred + " " + predSockConn.getInetAddress() + " " + predSockConn.getPort());
            new DataOutputStream(predSockConn.getOutputStream()).writeUTF("successorInfo: " + cursSucc + " " + succSockConn.getInetAddress() + " " + succSockConn.getPort());
        }
    }

    private void transferKeysToSuccessor(int selfID, int predID, int succID) throws IOException {
        String resp;
        DataInputStream nsDis = new DataInputStream(nsConnAll.get(selfID).getInputStream());
        DataOutputStream nsDos = new DataOutputStream(nsConnAll.get(selfID).getOutputStream());
        nsDos.writeUTF("send KV from " + predID + " to " + selfID + "-1"); // tranfer from predID to selfID(exclusive)

        if(succID != 0) {
            DataOutputStream succDos = new DataOutputStream(nsConnAll.get(succID).getOutputStream());
            while(true) {
                 resp = nsDis.readUTF();
                 if(!resp.equals("end transfer")) {
                     succDos.writeUTF(resp); //resp format "Key Value"
                 } else break;
            }
        } else {
            while(true) {
                resp = nsDis.readUTF();
                if(!resp.equals("end transfer")) {
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
            if(cmd.trim().split(" ")[0].equals("lookup")) {
                try {
                    bootstrap.nsOperations.lookup(Integer.parseInt(cmd.trim().split(" ")[1]), ""); // hopstart at bootstrap server with 0 as ID
                } catch (IOException | ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            } else if (cmd.trim().split(" ")[0].equals("insert")) {
                try {
                    bootstrap.nsOperations.insert(Integer.parseInt(cmd.trim().split(" ")[1]), cmd.trim().split(" ")[2], ""); // hopstart at bootstrap server with 0 as ID
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else if (cmd.trim().split(" ")[0].equals("delete")) {
                try {
                    bootstrap.nsOperations.delete(Integer.parseInt(cmd.trim().split(" ")[1]), ""); // hopstart at bootstrap server with 0 as ID
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                System.out.println("Error: command not found..");
            }
        }

    }
}
