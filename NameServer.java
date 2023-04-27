import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
public class NameServer {
    HashMap<Integer, String> data = new HashMap<>();
    static ServerSocket serverSocket;
    static Socket bootstrapSocket, socket;
    static int nsID;
    static int nsPort;
    static int bootstrapPort;
    static String bootstrapIP;
    NSOperations nsOperations;
    static DataInputStream dis;
    static DataOutputStream dos;
    public NameServer() throws IOException {
        this.nsOperations = new NSOperations(data, nsID, nsPort);
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException {
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

        serverSocket = new ServerSocket(nsPort);
        while(true) {
            socket = serverSocket.accept();
            dis = new DataInputStream(socket.getInputStream());
            dos = new DataOutputStream(socket.getOutputStream());

            String msg = dis.readUTF();
            if(!msg.startsWith("update")) {
                String hopInfo = dis.readUTF();
                if(msg.split(" ")[0].equals("lookup")) {
                    nameServer.nsOperations.lookup(Integer.parseInt(msg.split(" ")[1]), hopInfo);
                } else if (msg.split(" ")[0].equals("insert")) {
                    nameServer.nsOperations.insert(Integer.parseInt(msg.split(" ")[1]), msg.split(" ")[2], hopInfo);
                } else if (msg.split(" ")[0].equals("delete")) {
                    nameServer.nsOperations.lookup(Integer.parseInt(msg.split(" ")[1]), hopInfo);
                }
            }


        }
    }


    public void enterRing() throws UnknownHostException, IOException {
        System.out.println("Info: entering system..");
        bootstrapSocket = new Socket(bootstrapIP, bootstrapPort);
//        DataOutputStream bdos = new DataOutputStream(bootstrapSocket.getOutputStream());
        DataInputStream bdis = new DataInputStream(bootstrapSocket.getInputStream());
//        DataOutputStream dos = new DataOutputStream(nsSocket.getOutputStream());
//        DataInputStream dis = new DataInputStream(nsSocket.getInputStream());

        dos.writeUTF("enter " + nsOperations.nsMeta.getID() + " " + nsOperations.nsMeta.getIP() + " " + nsOperations.nsMeta.getServerPort());

        while(true) {
            String resp = bdis.readUTF();
            if(resp.equals("end transfer"))
                break;
            data.put(Integer.valueOf(resp.split(" ")[0]), resp.split(" ")[1]);
//            System.out.println(resp);
        }

        String predecessorInfo = bdis.readUTF();
        String successorInfo = bdis.readUTF();

        int predecessorID = Integer.parseInt(predecessorInfo.split(" ")[1]);
        String predecessorIP = predecessorInfo.split(" ")[1];
        int predecessorPort = Integer.parseInt(predecessorInfo.split(" ")[1]);
        int successorID = Integer.parseInt(successorInfo.split(" ")[1]);
        String successorIP = successorInfo.split(" ")[1];
        int successorPort = Integer.parseInt(successorInfo.split(" ")[1]);

        this.nsOperations.nsMeta.setRingMeta(predecessorID, predecessorIP, predecessorPort, successorID, successorIP, successorPort);
        System.out.println(this.nsOperations.nsMeta.getSuccessorID());
        System.out.println(this.nsOperations.nsMeta.getPredecessorID());
    }
    
    public void exitRing() {
    }
}
class NameServerUI implements Runnable{
    NameServer nameServer;
    public  NameServerUI(NameServer nameServer) {
        this.nameServer = nameServer;
    }

    @Override
    public void run() {
        String cmd = "";
        Scanner userInput = new Scanner(System.in);
        while(true) {
            System.out.print("nameserver" + nameServer.nsOperations.nsMeta.getID() + "$> ");
            cmd = userInput.nextLine();
            if(cmd.trim().equals("enter")) {
                try {
                    nameServer.enterRing();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else if (cmd.trim().equals("exit")) {
                nameServer.exitRing();
            } else {
                System.out.println(cmd + ": command not found..");
            }
        }
    }
}
