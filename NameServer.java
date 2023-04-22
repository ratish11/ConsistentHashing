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
    static ServerSocket serverSocket;
    static Socket socket;
    static int ID;
    static int serverPort;
    NSOperations nsOperations;

    public NameServer() {
        nsOperations = new NSOperations(data, ID, serverPort);
    }


    public static void main(String[] args) throws IOException {
        File confFile = new File(args[0]);
//        read config from file
        Scanner scanner = new Scanner(confFile);
        ID = Integer.parseInt(scanner.nextLine());
        serverPort = Integer.parseInt(scanner.nextLine());
        serverSocket = new ServerSocket(serverPort);
        NameServer nameServer = new NameServer();
        new Thread(new NameServerUI(nameServer)).start();
    }

    public void enterRing() {
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
//            need to add insert and delete methods
//            else if (cmd.trim().equals("insert")) {
//                try {
//                    bootstrap.nsOperations.
//                }
//            }
        }

    }

}
