package comp90015.idxsrv.peer;


import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.concurrent.LinkedBlockingDeque;

import comp90015.idxsrv.filemgr.FileDescr;
import comp90015.idxsrv.filemgr.FileMgr;
import comp90015.idxsrv.message.*;
import comp90015.idxsrv.server.IOThread;
import comp90015.idxsrv.server.IndexElement;
import comp90015.idxsrv.textgui.ISharerGUI;
import org.json.JSONObject;
import org.json.JSONString;

/**
 * Skeleton Peer class to be completed for Project 1.
 * @author aaron
 *
 */
public class Peer implements IPeer {

	private IOThread ioThread;
	
	private LinkedBlockingDeque<Socket> incomingConnections;
	
	private ISharerGUI tgui;
	
	private String basedir;
	
	private int timeout;
	
	private int port;
	
	public Peer(int port, String basedir, int socketTimeout, ISharerGUI tgui) throws IOException {
		this.tgui=tgui;
		this.port=port;
		this.timeout=socketTimeout;
		this.basedir=new File(basedir).getCanonicalPath();
		ioThread = new IOThread(port,incomingConnections,socketTimeout,tgui);
		ioThread.start();
	}
	
	public void shutdown() throws InterruptedException, IOException {
		ioThread.shutdown();
		ioThread.interrupt();
		ioThread.join();
	}
	
	/*
	 * Students are to implement the interface below.
	 */
	
	@Override
	public void shareFileWithIdxServer(File file, InetAddress idxAddress, int idxPort, String idxSecret,
			String shareSecret) {
		try {
			Socket socket = new Socket(idxAddress, idxPort);
			BufferedReader bufferedReader = getBufferedReader(socket);
			BufferedWriter bufferedWriter = getBufferedWriter(socket);

			String jsonStr = bufferedReader.readLine();
			if(jsonStr!=null) {
				WelcomeMsg welcome = (WelcomeMsg) MessageFactory.deserialize(jsonStr);
				String welcomeStr = welcome.msg;
				tgui.logInfo("server welcome: "+welcomeStr);
			}

			Message idxSecretMsg = new AuthenticateRequest(idxSecret);
			bufferedWriter.write(idxSecretMsg.toString());
			bufferedWriter.newLine();
			bufferedWriter.flush();


			jsonStr = bufferedReader.readLine();
			if(jsonStr!=null){
				AuthenticateReply check = (AuthenticateReply) MessageFactory.deserialize(jsonStr);
				Boolean success = check.success;
				if (!success){
					tgui.logError("Cannot match IdxSecret");
					return;
				}
			}
			String fileName = file.getName();
			RandomAccessFile randomAccessFile = new RandomAccessFile(file,"r");
			FileDescr fileDescr = new FileDescr(randomAccessFile);
			ShareRequest shareRequest = new ShareRequest(fileDescr, fileName, shareSecret, port);
			if(shareRequest.toString()==null){
				tgui.logError("Check file, it is an empty file!");
				return;
			}
			bufferedWriter.write(shareRequest.toString());
			bufferedWriter.newLine();
			bufferedWriter.flush();

			jsonStr = bufferedReader.readLine();
			if(jsonStr!=null){
				ShareReply shareReply = (ShareReply) MessageFactory.deserialize(jsonStr);
				String relativePath = file.getCanonicalPath();
				int numberShares = shareReply.numSharers;
				FileMgr fileMgr = new FileMgr(fileName, fileDescr);
				ShareRecord shareRecord = new ShareRecord(fileMgr, numberShares, "seeding", idxAddress,idxPort,idxSecret,shareSecret );
				tgui.addShareRecord(relativePath,shareRecord);
			}
		} catch (IOException | JsonSerializationException | NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
//		tgui.logError("shareFileWithIdxServer unimplemented");
	}

	@Override
	public void searchIdxServer(String[] keywords, 
			int maxhits, 
			InetAddress idxAddress, 
			int idxPort, 
			String idxSecret) {
		tgui.clearSearchHits();
		try(Socket socket = new Socket(idxAddress, idxPort)) {
			BufferedReader bufferedReader = getBufferedReader(socket);
			BufferedWriter bufferedWriter = getBufferedWriter(socket);

			String jsonStr = bufferedReader.readLine();
			if (jsonStr != null) {
				Message msg = (Message) MessageFactory.deserialize(jsonStr);
			}
			Message mess = new AuthenticateRequest(idxSecret);
			bufferedWriter.write(mess.toString());
			bufferedWriter.newLine();
			bufferedWriter.flush();

			jsonStr = bufferedReader.readLine();
			if (jsonStr != null) {
				Message msg = (Message) MessageFactory.deserialize(jsonStr);
				AuthenticateReply authenticateReply = (AuthenticateReply) msg;
				Boolean success = authenticateReply.success;
				if (!success){
					tgui.logError("Cannot match IdxSecret");
					return;
				}

			}

			SearchRequest searchRequest = new SearchRequest(maxhits,keywords);
			bufferedWriter.write(searchRequest.toString());
			bufferedWriter.newLine();
			bufferedWriter.flush();

			jsonStr = bufferedReader.readLine();
			if(jsonStr!=null) {
				Message msg = (Message) MessageFactory.deserialize(jsonStr);
				SearchReply searchReply = (SearchReply) msg;

				int count =0;
				while(count<searchReply.seedCounts.length){
					IndexElement hititem = new IndexElement();
					hititem = searchReply.hits[count];
					SearchRecord searchRecord = new SearchRecord(searchReply.hits[count].fileDescr,searchReply.seedCounts[count],
							idxAddress,idxPort,idxSecret,hititem.secret);
					tgui.addSearchHit(hititem.filename,searchRecord);
					count++;
				}
			}
		}catch (Exception e){
			tgui.logError("Search fail");
		}


		//tgui.logError("searchIdxServer unimplemented");
	}

	@Override
	public boolean dropShareWithIdxServer(String relativePathname, ShareRecord shareRecord) {
		//tgui.logError("dropShareWithIdxServer unimplemented");
		InetAddress idxAddress = shareRecord.idxSrvAddress;
		int idxPort = shareRecord.idxSrvPort;
		String idxSecret = shareRecord.idxSrvSecret;
		try(Socket socket = new Socket(idxAddress, idxPort)) {
			BufferedReader bufferedReader = getBufferedReader(socket);
			BufferedWriter bufferedWriter = getBufferedWriter(socket);


			String jsonStr = bufferedReader.readLine();
			if (jsonStr != null) {
				Message msg = (Message) MessageFactory.deserialize(jsonStr);
			}
			Message mess = new AuthenticateRequest(idxSecret);
			bufferedWriter.write(mess.toString());
			bufferedWriter.newLine();
			bufferedWriter.flush();

			jsonStr = bufferedReader.readLine();
			if (jsonStr != null) {
				Message msg = (Message) MessageFactory.deserialize(jsonStr);
				AuthenticateReply authenticateReply = (AuthenticateReply) msg;
				Boolean success = authenticateReply.success;
				if (!success){
					tgui.logError("Cannot match IdxSecret");
					return false;
				}
			}

			String fileName = relativePathname.substring(relativePathname.lastIndexOf("/")+1);
			String fileMd5 = shareRecord.fileMgr.getFileDescr().getFileMd5();
			String sharerSscret = shareRecord.sharerSecret;

			DropShareRequest dropShareRequest = new DropShareRequest(fileName,fileMd5,sharerSscret,port);
			bufferedWriter.write(dropShareRequest.toString());
			bufferedWriter.newLine();
			bufferedWriter.flush();

			jsonStr = bufferedReader.readLine();
			Message msg = null;
			if(jsonStr!=null) {
				msg = (Message) MessageFactory.deserialize(jsonStr);
			}
			DropShareReply dropShareReply = (DropShareReply) msg;
			tgui.logInfo("The file share has been dropped");
			return dropShareReply.success;
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (JsonSerializationException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void downloadFromPeers(String relativePathname, SearchRecord searchRecord) {

		InetAddress idxAddress = searchRecord.idxSrvAddress;
		int idxPort = searchRecord.idxSrvPort;
		String idxSecret = searchRecord.idxSrvSecret;

		try {
			Socket socket = new Socket(idxAddress, idxPort);
			BufferedReader bufferedReader = getBufferedReader(socket);
			BufferedWriter bufferedWriter = getBufferedWriter(socket);

			String jsonStr = bufferedReader.readLine();
			if(jsonStr!=null) {
				WelcomeMsg welcome = (WelcomeMsg) MessageFactory.deserialize(jsonStr);
				String welcomeStr = welcome.msg;
				tgui.logInfo("server welcome: "+welcomeStr);
			}

			Message idxSecretMsg = new AuthenticateRequest(idxSecret);
			bufferedWriter.write(idxSecretMsg.toString());
			bufferedWriter.newLine();
			bufferedWriter.flush();


			jsonStr = bufferedReader.readLine();
			if(jsonStr!=null){
				AuthenticateReply check = (AuthenticateReply) MessageFactory.deserialize(jsonStr);
				Boolean success = check.success;
				if (!success){
					tgui.logError("Cannot match IdxSecret");
					return;
				}
			}

			String fileName = relativePathname.substring(relativePathname.lastIndexOf("/")+1);
			FileDescr fileDescr = searchRecord.fileDescr;
			String fileMd5 = fileDescr.getFileMd5();
			LookupRequest lookupRequest = new LookupRequest(fileName, fileMd5);
			bufferedWriter.write(lookupRequest.toString());
			bufferedWriter.newLine();
			bufferedWriter.flush();

			jsonStr = bufferedReader.readLine();
			LookupReply lookupReply =null;
			if(jsonStr!=null){
				lookupReply = (LookupReply) MessageFactory.deserialize(jsonStr);
			}

			IndexElement[] peerServers = lookupReply.hits;
			int numberOfTotalBlocks = searchRecord.fileDescr.getNumBlocks();
			int numberOfPeerServers = peerServers.length;
			PeerClient peerClient = new PeerClient(peerServers, numberOfTotalBlocks,numberOfPeerServers, relativePathname, tgui);
			peerClient.start();
		} catch (IOException | JsonSerializationException e) {
			throw new RuntimeException(e);
		}


//		tgui.logError("downloadFromPeers unimplemented");
	}

	public BufferedReader getBufferedReader(Socket socket) throws IOException {

		InputStream inputStream = socket.getInputStream();
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
		return bufferedReader;
	}

	public BufferedWriter getBufferedWriter(Socket socket) throws IOException {

		OutputStream outputStream = socket.getOutputStream();
		BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
		return bufferedWriter;
	}
	
}
