import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Inet4Address;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

public class NSInterface extends Thread implements Serializable  {
	private static ServerSocket server;
	static Socket socket;
	static ObjectOutputStream outputStream;
	static ObjectInputStream inputStream;
	static HashMap<Integer, String> data = new HashMap<>();
	static NSMeta nsMeta = null;
	NameServer nameServer;
	static Socket fwdSocket = null;
	public NSInterface(NameServer nameServer) {
		this.nameServer = nameServer;
	}

	public void run() {
		try {
			//	System.out.println("Port for connection : "+nameServer.nsMeta.serverPortForConnection+"\n");
			int port = nameServer.nsMeta.getServerPort();
			server = new ServerSocket(port);

			while(true) {
				System.out.print("\nnameserver" + nameServer.nsMeta.getID() + "SH> ");
				socket = server.accept();
				outputStream = new  ObjectOutputStream(socket.getOutputStream());
				inputStream = new ObjectInputStream(socket.getInputStream());
				String nameServerDetails = (String) inputStream.readObject();
				String[] nameServerDetailsStr = nameServerDetails.split(" ");
				System.out.println("\ncommand received: "+nameServerDetails);

				String nextServerIP;
				int nextServerListeningPort;
				switch(nameServerDetailsStr[0]) {
					case "enter":

						int newNSId = Integer.parseInt(nameServerDetailsStr[1]);
						String newNSIP = nameServerDetailsStr[2];
						int newNSListeningPort = Integer.parseInt(nameServerDetailsStr[3]);
						if(newNSId < nameServer.nsMeta.getID()) {//its in between
							System.out.println("I'm here");
							outputStream.writeObject(nameServer.nsMeta.getServerPort());//write successor
							outputStream.writeObject(nameServer.nsMeta.getPredecessorPort()); //write predessor ip
							outputStream.writeObject(nameServer.nsMeta.getID());//write successor id
							outputStream.writeObject(nameServer.nsMeta.getPredecessorID());//write preccessor id
							outputStream.writeObject(Inet4Address.getLocalHost().getHostAddress()); //write successor ip
							outputStream.writeObject(nameServer.nsMeta.getPredecessorIP()); //write predessor ip
							System.out.println("Sending keys from in range ["+nameServer.nsMeta.getPredecessorID()+","+newNSId+")");
							for(int key = nameServer.nsMeta.getPredecessorID(); key < newNSId; key++) {

								if(nameServer.data.containsKey(key)) {
									//System.out.println(key);
									outputStream.writeObject(key);
									outputStream.writeObject(nameServer.data.get(key));
									nameServer.data.remove(key);
								}

							}
							nameServer.nsMeta.putPredecessorID(newNSId);
							nameServer.nsMeta.putPredecessorIP(newNSIP);
							nameServer.nsMeta.putPredecessorPort(newNSListeningPort);
							outputStream.writeObject(-1);
						} else if(newNSId > nameServer.nsMeta.getID()) {
							System.out.println("else here");
							//after current server
							nextServerListeningPort = nameServer.nsMeta.getSuccessorPort();
							nextServerIP = nameServer.nsMeta.getSuccessorIP();
							int nextServerId = nameServer.nsMeta.getSuccessorID();

							//1)if new nameserver is between current server and successor and update the successor of current server
							if(nameServer.nsMeta.getSuccessorID() > newNSId)
								nameServer.nsMeta.updateSuccessor(newNSId, newNSIP, newNSListeningPort);


							fwdSocket = new Socket(nextServerIP, nextServerListeningPort);

							ObjectInputStream inputStreamFwd = new ObjectInputStream(fwdSocket.getInputStream());
							ObjectOutputStream outputStreamFwd = new ObjectOutputStream(fwdSocket.getOutputStream());
							outputStreamFwd.writeObject("enter "+newNSId + " "+ newNSIP + " " + newNSListeningPort);

							int successorPortListning = (int) inputStreamFwd.readObject();
							outputStream.writeObject(successorPortListning);//send successor port
							int predessorPortListning = (int) inputStreamFwd.readObject();
							outputStream.writeObject(predessorPortListning);//send predessor port
							int successorId = (int) inputStreamFwd.readObject();
							outputStream.writeObject(successorId);//send sender id
							int predessorId = (int) inputStreamFwd.readObject();
							outputStream.writeObject(predessorId);//send predessor id
							String successorIP = (String) inputStreamFwd.readObject();
							outputStream.writeObject(successorIP);//send successor ip
							String predessorIP = (String) inputStreamFwd.readObject();
							outputStream.writeObject(predessorIP);//send predessor ip

							while(true) {

								int key =  (int) inputStreamFwd.readObject();
								outputStream.writeObject(key);
								if(key == -1)
									break;

								String value = (String) inputStreamFwd.readObject();
								outputStream.writeObject(value);

							}

							fwdSocket.close();
							System.out.println("done");
						}
						break;

					case "enterAtLast":
						System.out.println("Fitting the new server in b/w Bootstrap and Bootstrap's predecessor");

						newNSId = Integer.parseInt(nameServerDetailsStr[1]);
						newNSIP = nameServerDetailsStr[2];
						newNSListeningPort = Integer.parseInt(nameServerDetailsStr[3]);

						nextServerIP = nameServer.nsMeta.getSuccessorIP();
						nextServerListeningPort = nameServer.nsMeta.getSuccessorPort();
						System.out.println("Info in NS: creating socket with "+nameServer.nsMeta.getSuccessorID());

						if(newNSId > nameServer.nsMeta.getID() && nameServer.nsMeta.getSuccessorID() != 0) {
							fwdSocket = new Socket(nextServerIP, nextServerListeningPort);
							ObjectInputStream inputStreamFwd = new ObjectInputStream(fwdSocket.getInputStream());
							ObjectOutputStream outputStreamFwd = new ObjectOutputStream(fwdSocket.getOutputStream());
							outputStreamFwd.writeObject("enterAtLast "+newNSId + " "+ newNSIP + " " + newNSListeningPort);

							nextServerListeningPort = (int) inputStreamFwd.readObject();
							int predessorPortListning = (int) inputStreamFwd.readObject();
							int successorID = (int) inputStreamFwd.readObject();
							int PredessorID = (int) inputStreamFwd.readObject();
							nextServerIP = (String) inputStreamFwd.readObject();
							String PredessorIP = (String) inputStreamFwd.readObject();

							outputStream.writeObject(nextServerListeningPort);//send successor port
							outputStream.writeObject(predessorPortListning);//send predessor port
							outputStream.writeObject(successorID);//send successor id
							outputStream.writeObject(PredessorID);//send predessor id
							outputStream.writeObject(nextServerIP);//send successor ip
							outputStream.writeObject(PredessorIP);//send predessor ip

						}
						else {
							outputStream.writeObject(nextServerListeningPort);//send successor port
							outputStream.writeObject(nameServer.nsMeta.getPredecessorPort());//send predessor port
							outputStream.writeObject(nameServer.nsMeta.getSuccessorID());//send successor id
							outputStream.writeObject(nameServer.nsMeta.getID());//send predessor id
							outputStream.writeObject(nextServerIP);//send successor ip
							outputStream.writeObject(nameServer.nsMeta.getPredecessorIP());//send predessor ip

							System.out.println("Successor Id " + nameServer.nsMeta.getSuccessorID());
							//update successor of current server
							nameServer.nsMeta.updateSuccessor(newNSId,  newNSIP, newNSListeningPort);
						}
						break;

					case "updateYourPredessorAndTakeAllKeys":
//						System.out.println("In Successor to updateYourPredessorAndTakeAllKeys");

						int predessorPort = (int) inputStream.readObject();//update successor port
						int predessorId = (int) inputStream.readObject();//update successor id
						String predessorIP = (String) inputStream.readObject();//update successor ip
						nameServer.nsMeta.updatePredecessor(predessorId, predessorIP, predessorPort);
						while(true) {
							int key =  (int) inputStream.readObject();
							if(key == -1) break;
							String value = (String) inputStream.readObject();
							nameServer.data.put(key, value);

						}
//						System.out.println("Updated Informatio successorId" + nameServer.nsMeta.getSuccessorID());

						break;
					case "lookup":
//						System.out.println("In 2nd server lookup");
						int key = Integer.parseInt(nameServerDetailsStr[1]);
						String serverTracker = (String) inputStream.readObject();
						String[] value = nameServer.lookup(key,serverTracker).split(" ");
						if(value.length>1)
							serverTracker = serverTracker.concat("->"+value[1]);
						else
							serverTracker = serverTracker.concat("->"+nameServer.nsMeta.getID());
						outputStream.writeObject(value[0]);
						outputStream.writeObject(serverTracker);
						break;

					case "delete":
//						System.out.println("In 2nd server delete");
						key = Integer.parseInt(nameServerDetailsStr[1]);
						String val = nameServer.delete(key);
						outputStream.writeObject(nameServer.nsMeta.getID()+"->"+val);
						break;

					case "insert":
						System.out.println("In 2nd server insert");
						key = Integer.parseInt(nameServerDetailsStr[1]);
						String valueToInsert;
						valueToInsert = nameServerDetailsStr[2];
						val = nameServer.insert(key, valueToInsert);
						outputStream.writeObject(nameServer.nsMeta.getID()+"->"+val);

						break;

					case "updateYourSuccessor":
						System.out.println("In Predessor to updateYourSuccessor");
						int successorPort = (int) inputStream.readObject();//update predessor port
						int successorId = (int) inputStream.readObject();//update predessor id
						String successorIP = (String) inputStream.readObject();//update predessor ip
						nameServer.nsMeta.updateSuccessor(successorId, successorIP,successorPort);
						break;
					default:
						System.out.println("Error: invalid command received");


				}
				nameServer.nsMeta.printInfo();
			}
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
