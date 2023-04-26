import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

public class NameServer {
    HashMap<Integer, String> data = new HashMap<>();
    static ServerSocket nsSocket;
    static Socket bootStrapSocket;
    static int nsID;
    static int nsPort;
    static int bootStrapPort;
    static String bootStrapIP;
    NSOperations nsOperations;


    public NameServer() {
        nsOperations = new NSOperations(data, nsID, nsPort);
    }


    public static void main(String[] args) throws IOException {
        File confFile = new File(args[0]);
//        read config from file
        Scanner scanner = new Scanner(confFile);
        nsID = Integer.parseInt(scanner.nextLine());
        nsPort = Integer.parseInt(scanner.nextLine());
        String bs = scanner.nextLine();
        bootStrapIP = bs.split(" ")[0];
        bootStrapPort = Integer.parseInt(bs.split(" ")[1]);

        NameServer nameServer = new NameServer();
        new Thread(new NameServerUI(nameServer)).start();
    }
    
    public void enterRing() throws UnknownHostException, IOException {
        bootStrapSocket = new Socket(bootStrapIP, bootStrapPort);
        DataOutputStream dos = new DataOutputStream(bootStrapSocket.getOutputStream());
        DataInputStream dis = new DataInputStream(bootStrapSocket.getInputStream());
        //String nsIP = Inet4Address.getLocalHost().getHostAddress();
        String nsIP = nsOperations.nsMeta.getIP();

        dos.writeUTF("enter " + nsID + nsIP + nsPort);

        String predInfo = dis.readUTF();
        String succInfo = dis.readUTF();


        int predecessorID = Integer.parseInt(predInfo.split(" ")[1]);
        String predecessorIP = predInfo.split(" ")[1];
        int predecessorPort = Integer.parseInt(predInfo.split(" ")[1]);
        int successorID = Integer.parseInt(succInfo.split(" ")[1]);
        String successorIP = succInfo.split(" ")[1];
        int successorPort = Integer.parseInt(succInfo.split(" ")[1]);

        // NSMeta nsMeta = new NSMeta(nsID, nsPort);
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
            System.out.println("nameserver" + nameServer.ID + "-$> ");
            cmd = userInput.nextLine();
            if(cmd.trim().equals("entry")) {
                nameServer.enterRing();
            } else if (cmd.trim().equals("exit")) {
                nameServer.exitRing();
            } else {
                System.out.println(cmd + ": command not found..");
            }
        }
    }
}
