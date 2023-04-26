import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
public class NameServer {
    HashMap<Integer, String> data = new HashMap<>();
    static ServerSocket nsSocket;
    static Socket bootstrapSocket;
    static int nsID;
    static int nsPort;
    static int bootstrapPort;
    static String bootstrapIP;
    NSOperations nsOperations;
    public NameServer() {
        this.nsOperations = new NSOperations(data, nsID, nsPort);
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
    }
    public void enterRing() throws UnknownHostException, IOException {
        bootstrapSocket = new Socket(bootstrapIP, bootstrapPort);
        DataOutputStream dos = new DataOutputStream(bootstrapSocket.getOutputStream());
        DataInputStream dis = new DataInputStream(bootstrapSocket.getInputStream());
        //String nsIP = Inet4Address.getLocalHost().getHostAddress();

        dos.writeUTF("enter " + nsOperations.nsMeta.getID() + " " + nsOperations.nsMeta.getIP() + " " + nsOperations.nsMeta.getServerPort());
        String predecessorInfo = dis.readUTF();
        String successorInfo = dis.readUTF();

        int predecessorID = Integer.parseInt(predecessorInfo.split(" ")[1]);
        String predecessorIP = predecessorInfo.split(" ")[1];
        int predecessorPort = Integer.parseInt(predecessorInfo.split(" ")[1]);
        int successorID = Integer.parseInt(successorInfo.split(" ")[1]);
        String successorIP = successorInfo.split(" ")[1];
        int successorPort = Integer.parseInt(successorInfo.split(" ")[1]);

        nsOperations.nsMeta.setRingMeta(predecessorID, predecessorIP, predecessorPort, successorID, successorIP, successorPort);
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
            System.out.println("nameserver" + nameServer.nsOperations.nsMeta.getID() + "-$> ");
            cmd = userInput.nextLine();
            if(cmd.trim().equals("entry")) {
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
