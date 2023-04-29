import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Inet4Address;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import java.util.*;


public class Bootstrap implements Serializable  {

	private static ServerSocket serverSocket;
	private static int serverPort, ID;
	static Socket socket, nxtSrvSocket;
	static ArrayList<Integer> serverIDS = new ArrayList<>();
	HashMap<Integer, String> data = new HashMap<>();
	NSMeta nsMeta;
	public Bootstrap(){
		nsMeta = new NSMeta(ID,serverPort, data);
		serverIDS.add(0);
	}

	String lookup(int key) throws UnknownHostException, IOException, ClassNotFoundException {
		if(key>= Collections.max(serverIDS)) {
			if(data.containsKey(key)) {
				System.out.println("Server Visited 0"  );
				return (data.get(key));
			} else {
				return "Error: Key not found";
			}
		}

		nxtSrvSocket = new Socket(nsMeta.getSuccessorIP(), nsMeta.getSuccessorPort());
		ObjectInputStream inputStreamFwd = new ObjectInputStream(nxtSrvSocket.getInputStream());
		ObjectOutputStream outputStreamFwd = new ObjectOutputStream(nxtSrvSocket.getOutputStream());
		outputStreamFwd.writeObject("lookup "+key);
		outputStreamFwd.writeObject("0");
		String value = (String) inputStreamFwd.readObject();
		String hopInfo = (String) inputStreamFwd.readObject();
		int count = 0;
		for(int i = 0; i < hopInfo.length(); i++)
		{
			if(hopInfo.charAt(i) == '-')
				count++;
		}
		Collections.sort(serverIDS);
		System.out.println("Server Visited : "  );
		for(int id : serverIDS) {
			if(count-1 < 0)
				System.out.println(id);
			else
				System.out.print(id + "->");

			count--;
			if(count< 0)
				break;
		}
		nxtSrvSocket.close();
		return value;
	}
	void insert(int key, String value) throws IOException, ClassNotFoundException {
		//check if the key should be in bootstrap
		if(key >= Collections.max(serverIDS)) {
			System.out.println("Server Visited 0"  );
			System.out.println("Key Inserted at 0"  );
			if(data.containsKey(key)) System.out.println("Overwriting key: " + key);
			data.put(key,value);
			nsMeta.printInfo();
		}

		else {
			//if no then contact successor
			nxtSrvSocket = new Socket(nsMeta.getSuccessorIP(), nsMeta.getSuccessorPort());
			ObjectInputStream inputStreamFwd = new ObjectInputStream(nxtSrvSocket.getInputStream());
			ObjectOutputStream outputStreamFwd = new ObjectOutputStream(nxtSrvSocket.getOutputStream());
			outputStreamFwd.writeObject("insert "+key+" "+value);
			String hopInfo = (String) inputStreamFwd.readObject();
			int count = 0;
			for(int i = 0; i < hopInfo.length(); i++)
			{
				if(hopInfo.charAt(i) == '-')
					count++;
			}
			Collections.sort(serverIDS);
			System.out.println("Server Visited : "  );
			for(int id : serverIDS) {
				if(count-1 < 0)
					System.out.println(id);
				else
					System.out.print(id + "->");

				count--;
				if(count< 0)
					break;
			}
			nxtSrvSocket.close();
		}
	}
	void delete(int key) throws UnknownHostException, IOException, ClassNotFoundException {

		//if key in bootstrap server then dekete
		if(key >= Collections.max(serverIDS)) {
			if(!data.containsKey(key)) {
				System.out.println("Error: Key not found");
				return;
			}
			System.out.println("Deletion Succesfull");
			System.out.println("Server Visited 0");
			
			data.remove(key);
			nsMeta.printInfo();
		}
		else {
			nxtSrvSocket = new Socket(nsMeta.getSuccessorIP(), nsMeta.getSuccessorPort());
			ObjectInputStream inputStreamFwd = new ObjectInputStream(nxtSrvSocket.getInputStream());
			ObjectOutputStream outputStreamFwd = new ObjectOutputStream(nxtSrvSocket.getOutputStream());
			outputStreamFwd.writeObject("delete "+key);
			String serverTracker = (String) inputStreamFwd.readObject();
			int count = 0;boolean notFound = false;
			for(int i = 0; i < serverTracker.length(); i++) {
				if(serverTracker.charAt(i) == '-')
					count++;
				else if(serverTracker.charAt(i) == 'N')
					notFound = true;
			}
			if(notFound)
				System.out.println("Key Not Found");
			else
				System.out.println("Deletion Succesful");
			Collections.sort(serverIDS);
			System.out.print("Server Visited : "  );
			for(int id : serverIDS) {
				if(count-1 < 0) System.out.println(id);
				else
					System.out.print(id + "->");
				count--;
				if(count< 0) break;
			}
			nxtSrvSocket.close();
		}
	}



	public static void main(String[] args) throws IOException, ClassNotFoundException {
		try {
			File configFile = new File(args[0]);
			Scanner sc = new Scanner(configFile);
			ID = Integer.parseInt(sc.nextLine());
			serverPort = Integer.parseInt(sc.nextLine());
			serverSocket = new ServerSocket(serverPort);//only for listening commands from nameServer
			Bootstrap bootstrap = new Bootstrap();
			new BootstrapUI(bootstrap).start();
			while (sc.hasNextLine()) {
				String line = sc.nextLine();
				bootstrap.data.put(Integer.parseInt(line.split(" ")[0]),line.split(" ")[1]);
			}
			int maxServerID = 0;
			while(true) {
				socket = serverSocket.accept();
				ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
				ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
				String msg = (String) inputStream.readObject();
				System.out.println("\n" + msg);
				String[] msgList = msg.split(" ");
				//System.out.println(nameServerDetailsStr[0]);
				int nsID = 0;
				int nsPort = 0;
				String nsIP = "";
				if(!msgList[0].equals("updateYourpredecessorAndTakeAllKeys")) {
					if( !msgList[0].equals("updateYourSuccessor") && !msgList[0].equals("updateMaxServerID") ) {
						nsID = Integer.parseInt(msgList[1]);
						nsIP = msgList[2];
						nsPort = Integer.parseInt(msgList[3]);
					}
				}
				switch(msgList[0]) {
					case "enter":
						bootstrap.serverIDS.add(nsID);
						outputStream.writeObject(bootstrap.nsMeta.getIP());
						outputStream.writeObject(serverPort);
						System.out.println("New NameServer Entering: " + nsID);
						Collections.sort(bootstrap.serverIDS);
						String hopInfo = "0";
						if(bootstrap.nsMeta.getSuccessorID() == 0) {//when only Bootstrap in system
							outputStream.writeObject(bootstrap.nsMeta.getServerPort());//send NS's successor port
							outputStream.writeObject(bootstrap.nsMeta.getServerPort());//send NS's predecessor port
							outputStream.writeObject(bootstrap.nsMeta.getID());//send NS's successor id
							outputStream.writeObject(bootstrap.nsMeta.getID());//send NS's predecessor id
							outputStream.writeObject(bootstrap.nsMeta.getIP());//send NS's successor  ip
							outputStream.writeObject(bootstrap.nsMeta.getIP());//send NS's predecessor ip
							bootstrap.nsMeta.setRingMeta(nsID, nsIP, nsPort, nsID, nsIP, nsPort);
							//give all the value from 0 to id
							for(int key = 0; key < nsID; key++) {
								if(bootstrap.data.containsKey(key)) {
									outputStream.writeObject(key);
									outputStream.writeObject(bootstrap.data.get(key));
									bootstrap.data.remove(key);
								}
							}
							outputStream.writeObject(-1);
							System.out.println("data transferred");
						}
						else if(maxServerID < nsID) {//this is the new predecessor of Bootstrap
							bootstrap.nsMeta.updatePredecessor(nsID, nsIP, nsPort); //update predecessor of bootstrap
							System.out.println("Info: creating socket with "+bootstrap.nsMeta.getSuccessorID());
							nxtSrvSocket = new Socket(bootstrap.nsMeta.getSuccessorIP(), bootstrap.nsMeta.getSuccessorPort());
							ObjectInputStream inputStreamFwd = new ObjectInputStream(nxtSrvSocket.getInputStream());
							ObjectOutputStream outputStreamFwd = new ObjectOutputStream(nxtSrvSocket.getOutputStream());
							outputStreamFwd.writeObject("enterAtLast "+nsID + " "+ nsIP + " " + nsPort);
							int successorPort = (int) inputStreamFwd.readObject();
							outputStream.writeObject(successorPort);//send successor port
							int predecessorPort = (int) inputStreamFwd.readObject();
							outputStream.writeObject(predecessorPort);//send predecessor port
							int successorId = (int) inputStreamFwd.readObject();
							outputStream.writeObject(successorId);//send successor id
							int predecessorId = (int) inputStreamFwd.readObject();
							outputStream.writeObject(predecessorId);//send predecessor id
							String successorIP = (String) inputStreamFwd.readObject();
							outputStream.writeObject(successorIP);//send sucessor ip
							String predecessorIP = (String) inputStreamFwd.readObject();
							outputStream.writeObject(predecessorIP);//send predecessor ip

							for(int key = maxServerID; key < nsID; key++) {

								if(bootstrap.data.containsKey(key)) {
									//System.out.println(key);
									outputStream.writeObject(key);
									outputStream.writeObject(bootstrap.data.get(key));
									bootstrap.data.remove(key);
								}
							}
							outputStream.writeObject(-1);
						}
						else {//if new ip is between two servers

							int nextServerListeningPort = bootstrap.nsMeta.getSuccessorPort();
							String nextServerIP = bootstrap.nsMeta.getSuccessorIP();
							System.out.println("Getting the details from " + bootstrap.nsMeta.getSuccessorID());

							//if new NS is before Bootstrap's successor
							if(bootstrap.nsMeta.getSuccessorID() > nsID)
								bootstrap.nsMeta.updateSuccessor(nsID,nsIP,nsPort);

							//new NS is after Bootstrap's current successor
							nxtSrvSocket = new Socket(nextServerIP, nextServerListeningPort);

							ObjectInputStream inputStreamFwd = new ObjectInputStream(nxtSrvSocket.getInputStream());
							ObjectOutputStream outputStreamFwd = new ObjectOutputStream(nxtSrvSocket.getOutputStream());
							outputStreamFwd.writeObject("enter "+nsID + " "+ nsIP + " " + nsPort);

							int successorPortListning = (int) inputStreamFwd.readObject();
							outputStream.writeObject(successorPortListning);//send successor port
							int predecessorPortListning = (int) inputStreamFwd.readObject();
							outputStream.writeObject(predecessorPortListning);//send predecessor port
							int successorId = (int) inputStreamFwd.readObject();
							outputStream.writeObject(successorId);//send successor id
							int predecessorId = (int) inputStreamFwd.readObject();
							outputStream.writeObject(predecessorId);//send predecessor id
							String successorIP = (String) inputStreamFwd.readObject();
							outputStream.writeObject(successorIP);//send successor ip
							String predecessorIP = (String) inputStreamFwd.readObject();
							outputStream.writeObject(predecessorIP);//send predecessor ip

							while(true) {
								int key =  (int) inputStreamFwd.readObject();
								outputStream.writeObject(key);
								if(key == -1) break;
								String value = (String) inputStreamFwd.readObject();
								outputStream.writeObject(value);
							}
							nxtSrvSocket.close();
							//					System.out.println("done");
						}
						outputStream.close();
						inputStream.close();
						socket.close();
						System.out.println("Nameserver " + nsID + " entered successfully..");
						break;

					case "updateYourpredecessorAndTakeAllKeys":
						System.out.println("In Successor to updateYourpredecessorAndTakeAllKeys");

						int predecessorPort = (int) inputStream.readObject();//update successor port
						int predecessorId = (int) inputStream.readObject();//update successor id
						String predecessorIP = (String) inputStream.readObject();//update successor ip
						bootstrap.nsMeta.updatePredecessor(predecessorId, predecessorIP, predecessorPort);
						while(true) {
							int key =  (int) inputStream.readObject();
							if(key == -1)
								break;
							String value = (String) inputStream.readObject();
							bootstrap.data.put(key, value);
							System.out.println("Key : "+key+" Value : "+value);
						}
						break;
					case "updateYourSuccessor":
						System.out.println("In predecessor to updateYourSuccessor");
						int successorPort = (int) inputStream.readObject();//update predecessor port
						int successorId = (int) inputStream.readObject();//update predecessor id
						String successorIP = (String) inputStream.readObject();//update predecessor ip
						bootstrap.nsMeta.updateSuccessor(successorId, successorIP, successorPort);

						break;
					case "updateMaxServerID":
						int exitedID = (int) inputStream.readObject();
						bootstrap.serverIDS.remove(Integer.valueOf(exitedID));
						System.out.println("Updating last ServerID in the ring..");
						break;
				}
				maxServerID = Collections.max(bootstrap.serverIDS);
				bootstrap.nsMeta.printInfo();
				System.out.print("bootstrapSH> ");
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
}

class BootstrapUI extends Thread implements Serializable {

	Bootstrap bootstrap;
	public BootstrapUI(Bootstrap bootstrap) {
		this.bootstrap = bootstrap;
	}

	@Override
	public void run() {

		String input = "";
		Scanner s = new Scanner(System.in);
		do {
			System.out.print("bootstrapSH> ");
			input = s.nextLine().trim();
			String[] commandAndValue = input.split(" ");

			switch(commandAndValue[0]) {

				case "lookup":
					//System.out.println("In Lookup");
					try {
						System.out.println(bootstrap.lookup(Integer.parseInt(commandAndValue[1])));
					} catch (NumberFormatException  |  ClassNotFoundException | IOException e) {
						e.printStackTrace();
					}
					break;
				case "insert":
					try {
						bootstrap.insert(Integer.parseInt(commandAndValue[1]), commandAndValue[2]);
					} catch (NumberFormatException | ClassNotFoundException | IOException e) {
						e.printStackTrace();
					}
					break;

				case "delete":
					try {
						bootstrap.delete(Integer.parseInt(commandAndValue[1]));
					} catch (NumberFormatException | ClassNotFoundException | IOException e) {
						e.printStackTrace();
					}
					break;
				default:
					System.out.println("Error: command not found");
			}
		}while(true);
	}
}