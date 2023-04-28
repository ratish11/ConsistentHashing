import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
public class NameServer {
    HashMap<Integer, String> data = new HashMap<>();
//    static ServerSocket serverSocket;
//    static Socket bootstrapSocket, socket;
    static int nsID;
    static int nsPort;
    static int bootstrapPort;
    static String bootstrapIP;
    NSOperations nsOperations;
    static DataInputStream dis;
    static DataOutputStream dos;
    public NameServer()  {
        this.nsOperations = new NSOperations(data, nsID, nsPort);
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        File confFile = new File(args[0]);
//        read config from file
        Scanner scanner = new Scanner(confFile);
        nsID = Integer.parseInt(scanner.nextLine());
        nsPort = Integer.parseInt(scanner.nextLine());
        String bs = scanner.nextLine();
        bootstrapIP = bs.split(" ")[0];
        bootstrapPort = Integer.parseInt(bs.split(" ")[1]);

        NameServer nameServer = new NameServer();
        new Thread(new NameServerUI(nameServer)).start();
        ServerSocket serverSocket = new ServerSocket(nsPort);
        Socket socket;
//        Boolean dStreamInit = false;
        while(true) {
            socket = serverSocket.accept();
//            if(!dStreamInit) {
                dis = new DataInputStream(socket.getInputStream());
                dos = new DataOutputStream(socket.getOutputStream());
//                dStreamInit = true;
//            }
            String msg = dis.readUTF();
            System.out.println("\n" + msg);
            if(msg.startsWith("enter")) { nameServer.enterRing(); }//nameServer.nsOperations.printInfo();
            else if(msg.startsWith("exit")) nameServer.exitRing();
            else if (msg.startsWith("sendKV")) {nameServer.sendKVtoPredcessor(msg,dis, dos); break;}
            else if(!msg.startsWith("update")) {
                String hopInfo = dis.readUTF();
                if(msg.split(" ")[0].equals("lookup")) {
                    String lookupResponse = nameServer.nsOperations.lookup(Integer.parseInt(msg.split(" ")[1]), hopInfo);
                    dos.writeUTF(lookupResponse.split(" ")[0]);
                    dos.writeUTF(lookupResponse.split(" ")[1]);
                } else if (msg.split(" ")[0].equals("insert")) {
                    String insertResponse = nameServer.nsOperations.insert(Integer.parseInt(msg.split(" ")[1]), msg.split(" ")[2], hopInfo);
                    dos.writeUTF(insertResponse);
                } else if (msg.split(" ")[0].equals("delete")) {
                    String deleteResponse = nameServer.nsOperations.lookup(Integer.parseInt(msg.split(" ")[1]), hopInfo);
                    dos.writeUTF(deleteResponse.split(" ")[0]);
                    dos.writeUTF(deleteResponse.split(" ")[1]);
                }
            } else if(msg.startsWith("updatePredecessor")) {
                System.out.println("now update predecessor " + msg);
                nameServer.updatePredecessor(msg);
            } else if(msg.startsWith("updateSuccessor")) {
                System.out.println("now update successor " + msg);
                nameServer.updateSuccessor(msg);
            }
            System.out.print("nameserver" + nameServer.nsOperations.nsMeta.getID() + "$>> ");
//            socket.close();
//            serverSocket.close();
        }
    }

    private void updatePredecessor(String msg) {
        int predecessorID = Integer.parseInt(msg.split(" ")[1]);
        String predecessorIP = msg.split(" ")[1];
        int predecessorPort = Integer.parseInt(msg.split(" ")[1]);
        this.nsOperations.nsMeta.updatePredecessor(predecessorID, predecessorIP, predecessorPort);

        this.nsOperations.printInfo();
    }

    private void updateSuccessor(String msg) {
        int successorID = Integer.parseInt(msg.split(" ")[1]);
        String successorIP = msg.split(" ")[1];
        int successorPort = Integer.parseInt(msg.split(" ")[1]);
        this.nsOperations.nsMeta.updateSuccessor(successorID, successorIP, successorPort);
        this.nsOperations.printInfo();
    }

    private void sendKVtoPredcessor(String msg, DataInputStream dis, DataOutputStream dos) throws IOException, InterruptedException {
        List<Integer> deleteKey = new ArrayList<>();
        int startKey = Integer.parseInt(msg.split(" ")[1]);
        int endKey = Integer.parseInt(msg.split(" ")[3]);
        for(int key = startKey; key<endKey; key++) {
            if(this.data.containsKey(key)) {
                dos.writeUTF(String.valueOf(key) + " " + this.data.get(key));
                deleteKey.add(key);
            }
        }
        dos.writeUTF("endTransfer");
        for(Integer k : deleteKey) this.data.remove(k);
        String resp = dis.readUTF();
        if(resp.startsWith("updatePredecessor")) updatePredecessor(resp);
//        Thread.sleep(100);
    }

    public void enterRing() throws IOException {
        System.out.println("\nInfo: entering system...");
        String resp;
        ServerSocket temp = new ServerSocket(nsPort);
//        temp.setReuseAddress(true);
        Socket tempSocket = temp.accept();
        System.out.println("temp server created");
        Socket bootstrapSocket = new Socket(bootstrapIP, bootstrapPort);
        DataOutputStream bdos = new DataOutputStream(bootstrapSocket.getOutputStream());
        DataInputStream tempdis = new DataInputStream(tempSocket.getInputStream());
        //send commands over BootStrap Server's accept and everything else with NS's ServerSocket
        bdos.writeUTF("enter " + this.nsOperations.nsMeta.getID() + " " + this.nsOperations.nsMeta.getIP() + " " + this.nsOperations.nsMeta.getServerPort() + " " + temp.getLocalPort());
        while(true) {
            resp = tempdis.readUTF();
            System.out.println(resp);
            if(resp.startsWith("Error")) {
                System.out.println(resp);
                return;
            }
            if(resp.equals("endTransfer")) {
                resp = tempdis.readUTF();
                if(resp.startsWith("updatePredecessor")) updatePredecessor(resp);
                resp = tempdis.readUTF();
                if (resp.startsWith("updateSuccessor")) updateSuccessor(resp);
                break;
            }
            this.data.put(Integer.valueOf(resp.split(" ")[0]), resp.split(" ")[1]);
        }
        bdos.close();
        bootstrapSocket.close();
        dis.close();
        tempSocket.close();
        temp.close();
    }

    public void exitRing() throws IOException {
        System.out.println("\nInfo: exiting system...");
        String resp;
        Socket bootstrapSocket = new Socket(bootstrapIP, bootstrapPort);
        DataOutputStream bdos = new DataOutputStream(bootstrapSocket.getOutputStream());
        DataInputStream bdis = new DataInputStream(bootstrapSocket.getInputStream());
        bdos.writeUTF("exit " + " " + this.nsOperations.nsMeta.getID() + " " + this.nsOperations.nsMeta.getIP() + " " + this.nsOperations.nsMeta.getServerPort());
        String response = bdis.readUTF();
        int predID = Integer.parseInt(response.split(" ")[1]);
        int selfID = Integer.parseInt(response.split(" ")[1]);
        for(int key = predID; key<selfID; key++) {
            if(data.containsKey(key)) {
                bdos.writeUTF(key + " " + data.get(key));
            }
            data.clear();
            bdos.writeUTF("endTransfer");
        }


    }
}
class NameServerUI implements Runnable{
    NameServer nameServer;
    public  NameServerUI(NameServer nameServer) {
        this.nameServer = nameServer;
    }

    @Override
    public void run() {
//        try {
//            Thread.sleep(100);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//        Socket selfSocket;
//        DataOutputStream selfDOS;
        String cmd = "";
        Scanner userInput = new Scanner(System.in);
        try {
            while(true) {
//                selfSocket = new Socket(nameServer.nsOperations.nsMeta.getIP(), nameServer.nsOperations.nsMeta.getServerPort());
//                selfDOS = new DataOutputStream(selfSocket.getOutputStream());
                System.out.print("nameserver" + nameServer.nsOperations.nsMeta.getID() + "$> ");
                cmd = userInput.nextLine();
                if(cmd.trim().equals("enter")) {
//                    selfDOS.writeUTF("enter");
                    nameServer.enterRing();
                } else if (cmd.trim().equals("exit")) {
//                    selfDOS.writeUTF("exit");
                    nameServer.exitRing();
                } else {
                    System.out.println(cmd + ": command not found..");
                }
//                selfDOS.close();
//                selfSocket.close();
            }
        } catch (IOException io) {
            io.printStackTrace();
        }

    }
}
