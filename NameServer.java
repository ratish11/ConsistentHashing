import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Inet4Address;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

public class NameServer implements Serializable  {

	static Socket socket;
	static ObjectOutputStream outputStream;
	static ObjectInputStream inputStream;
	static HashMap<Integer, String> data = new HashMap<>();
	static NSMeta nsMeta;
	static Socket nxtSrvSocket;

	String lookup(int key,String hopInfo) throws IOException, ClassNotFoundException {
		if(data.containsKey(key))
			return (data.get(key));
		else if(key > this.nsMeta.getID()) {
			try {
				nxtSrvSocket = new Socket(nsMeta.getSuccessorIP(), nsMeta.getSuccessorPort());
				ObjectInputStream inputStreamFwd = new ObjectInputStream(nxtSrvSocket.getInputStream());
				ObjectOutputStream outputStreamFwd = new ObjectOutputStream(nxtSrvSocket.getOutputStream());
				outputStreamFwd.writeObject("lookup "+key);
				outputStreamFwd.writeObject(hopInfo);
				String value = (String) inputStreamFwd.readObject();
				System.out.println("Got Value" + value);

				String ServerTracker = (String) inputStreamFwd.readObject();
				System.out.println("Server" + ServerTracker);
				System.out.println("Checking in successor" + nsMeta.getSuccessorID());
				nxtSrvSocket.close();
				return value + " "+ ServerTracker;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return "NoKeyFound";
	}
	String insert(int key, String value) throws UnknownHostException, IOException, ClassNotFoundException {
		//check if the key should be in bootstrap
		//check if the key should be in bootstrap
		if(key < nsMeta.getID()) {
			System.out.println("Key inserted" + key);
			if(data.containsKey(key)) System.out.println("Overwriting key: " + key);
			data.put(key,value);
			return ""+nsMeta.getID();
		}
		else if(key >= this.nsMeta.getID()) {
			//if no then contact successor
			nxtSrvSocket = new Socket(nsMeta.getSuccessorIP(), nsMeta.getSuccessorPort());
			ObjectInputStream inputStreamFwd = new ObjectInputStream(nxtSrvSocket.getInputStream());
			ObjectOutputStream outputStreamFwd = new ObjectOutputStream(nxtSrvSocket.getOutputStream());
			outputStreamFwd.writeObject("insert "+key+" "+value);
			outputStreamFwd.writeObject(nsMeta.getID());
			value = (String) inputStreamFwd.readObject();
			nxtSrvSocket.close();
			return value;
		}
		return null;
	}
	String delete(int key) throws IOException, ClassNotFoundException {

		//if key in bootstrap server then dekete
		if(key < nsMeta.getID())
			if(data.containsKey(key)) {
				data.remove(key);
				return ""+nsMeta.getID();

			}
			else {
				System.out.println("Error: Key not found");
				return "NoKeyFound";
			}

		else if(key > this.nsMeta.getID()) {

			nxtSrvSocket = new Socket(nsMeta.getSuccessorIP(), nsMeta.getSuccessorPort());
			ObjectInputStream inputStreamFwd = new ObjectInputStream(nxtSrvSocket.getInputStream());
			ObjectOutputStream outputStreamFwd = new ObjectOutputStream(nxtSrvSocket.getOutputStream());
			outputStreamFwd.writeObject("delete "+key);
			String value = (String) inputStreamFwd.readObject();
			nxtSrvSocket.close();
			return value;
			//else check in successor
		}
		return null;
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		List<String> configFile = Files.readAllLines(Paths.get(args[0]));
		NameServer nameServer = new NameServer();
		int id = Integer.parseInt(configFile.get(0));
		int listeningPort = Integer.parseInt(configFile.get(1));
		String serverIP = configFile.get(2).split(" ")[0];
		int serverPort = Integer.parseInt(configFile.get(2).split(" ")[1]);
		String input = "";
		String bootstrapIP = "";
		int bootstrapPort = 0;
		Scanner scanner = new Scanner(System.in);
		nameServer.nsMeta = new NSMeta(id, listeningPort, data);
		do {
			System.out.print("nameserver" + id + "SH> ");
			input = scanner.nextLine();
			String[] cmd = input.split(" ");
			switch(cmd[0]) {
				case "enter":
					System.out.println(serverIP +":"+serverPort);
					socket = new Socket(serverIP, serverPort);
					outputStream = new  ObjectOutputStream(socket.getOutputStream());
					inputStream = new ObjectInputStream(socket.getInputStream());
					String nameServerIP = nameServer.nsMeta.getIP();
					outputStream.writeObject("enter "+id + " "+ nameServerIP + " " + listeningPort);
					//push connection details to bootstrap
					bootstrapIP = (String) inputStream.readObject();
					bootstrapPort = (int) inputStream.readObject();
					System.out.println(bootstrapIP+":"+bootstrapPort);
//				String serverTracker = (String) inputStream.readObject();
					int successorPortListning = (int) inputStream.readObject();
					int predecessorPortListning = (int) inputStream.readObject();
					int successorId = (int) inputStream.readObject();
					int predecessorId = (int) inputStream.readObject();
//				System.out.println("my predecessorID is: " + predecessorId);
					String successorIP = (String) inputStream.readObject();
					String predecessorIP = (String) inputStream.readObject();

					nameServer.nsMeta.setRingMeta(predecessorId, predecessorIP, predecessorPortListning, successorId, successorIP, successorPortListning);
//				nameServer.nsInfo.id = id;
					//System.out.println("SuccessorId : " + successorId +" predecessorId " +predecessorId + "predecessorIP " + predecessorIP+" predecessorPort : "+predecessorPortListning);
					while(true) {
						int key =  (int) inputStream.readObject();
						if(key == -1) break;
						String value = (String) inputStream.readObject();
						nameServer.data.put(key, value);
					}
					outputStream.close();
					inputStream.close();
					socket.close();
					System.out.println("Keys received in the range ["+predecessorId+","+id+")");
					new NSInterface(nameServer).start();
					System.out.println("Successfully joined system.");
					break;
				case "exit":
					//give all keys to successor and tell him to update his predecessor
					socket = new Socket(nameServer.nsMeta.getSuccessorIP(), nameServer.nsMeta.getSuccessorPort());
					outputStream = new  ObjectOutputStream(socket.getOutputStream());
					inputStream = new ObjectInputStream(socket.getInputStream());

					outputStream.writeObject("updateYourpredecessorAndTakeAllKeys");//To successor
					outputStream.writeObject(nameServer.nsMeta.getPredecessorPort());//send predecessor port
					outputStream.writeObject(nameServer.nsMeta.getPredecessorID());//send predecessor id
					outputStream.writeObject(nameServer.nsMeta.getIP());//send predecessor Ip

					for(int key = nameServer.nsMeta.getPredecessorID(); key < nameServer.nsMeta.getID(); key++) {
						if(nameServer.data.containsKey(key)) {
							outputStream.writeObject(key);
							outputStream.writeObject(nameServer.data.get(key));
							nameServer.data.remove(key);
						}
					}
					outputStream.writeObject(-1);
					outputStream.close();
					inputStream.close();
					socket.close();

					/////////////////////////**************************/////////////////////////////
					//tell predecessor to update its successor
					socket = new Socket(nameServer.nsMeta.getPredecessorIP(), nameServer.nsMeta.getPredecessorPort());
					outputStream = new  ObjectOutputStream(socket.getOutputStream());
					inputStream = new ObjectInputStream(socket.getInputStream());

					outputStream.writeObject("updateYourSuccessor");//sned predecessor the new successor
					outputStream.writeObject(nameServer.nsMeta.getSuccessorPort());//send successor port
					outputStream.writeObject(nameServer.nsMeta.getSuccessorID());//send successor id
					outputStream.writeObject(nameServer.nsMeta.getSuccessorIP());//send successor Ip

					outputStream.close();
					inputStream.close();
					socket.close();
					// tell bootstrap that i am exiting
					socket = new Socket(bootstrapIP, bootstrapPort);
					outputStream = new  ObjectOutputStream(socket.getOutputStream());
					outputStream.writeObject("updateMaxServerID");
					outputStream.writeObject(id);
					socket.close();
					break;
				default:
					System.out.println("Error: command not found");
			}
			nameServer.nsMeta.printInfo();
//			System.out.println("NameServer SuccessorId : "+nameServer.nsMeta.getSuccessorID() + " predecessorId :"+nameServer.nsMeta.predecessorId);

		}while(!input.equals("exit"));
		System.out.println("NameServer Exited");
	}

}
