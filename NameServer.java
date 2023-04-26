import java.awt.*;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
public class NameServer {
    HashMap<Integer, String> data = new HashMap<>();
    static ServerSocket nsServerSocket;
    static Socket bootstrapSocket, nsSocket;
    static int nsID;
    static int nsPort;
    static int bootstrapPort;
    static String bootstrapIP;
    NSOperations nsOperations;
    public NameServer() throws IOException {
        this.nsOperations = new NSOperations(data, nsID, nsPort);
        this.nsServerSocket = new ServerSocket(nsPort);
        this.nsSocket = nsServerSocket.accept();
    }
    public static void main(String[] args) throws IOException {
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
//        nsService();
    }

//    private static void nsService() throws IOException {
//        ServerSocket
//        while (true) {
//            nsSocket = nsServerSocket.accept();
//            DataInputStream dis = new DataInputStream(nsSocket.getInputStream());
//            DataOutputStream dos = new DataOutputStream((nsSocket.getOutputStream()));
////            while(true) {
////                String resp = dis.readUTF();
////                if(resp.equals("end transfer"))
////                   break;
////                data.put(Integer.valueOf(resp.split(" ")[0]), resp.split(" ")[1])
//////                System.out.println(resp);
////            }
//        }
//    }

    public void enterRing() throws UnknownHostException, IOException {
        System.out.println("Info: entering system..");
        bootstrapSocket = new Socket(bootstrapIP, bootstrapPort);
        DataOutputStream bdos = new DataOutputStream(bootstrapSocket.getOutputStream());
        DataInputStream bdis = new DataInputStream(bootstrapSocket.getInputStream());
        DataOutputStream dos = new DataOutputStream(nsSocket.getOutputStream());
        DataInputStream dis = new DataInputStream(nsSocket.getInputStream());

        dos.writeUTF("enter " + nsOperations.nsMeta.getID() + " " + nsOperations.nsMeta.getIP() + " " + nsOperations.nsMeta.getServerPort());

        while(true) {
            String resp = dis.readUTF();
            if(resp.equals("end transfer"))
                break;
            data.put(Integer.valueOf(resp.split(" ")[0]), resp.split(" ")[1]);
//            System.out.println(resp);
        }

        String predecessorInfo = dis.readUTF();
        String successorInfo = dis.readUTF();

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
