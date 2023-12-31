package comp90015.idxsrv.peer;

import comp90015.idxsrv.filemgr.FileDescr;
import comp90015.idxsrv.filemgr.FileMgr;
import comp90015.idxsrv.message.*;
import comp90015.idxsrv.server.IndexElement;
import comp90015.idxsrv.textgui.ISharerGUI;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class PeerClient extends Thread{
    private IndexElement[] peerServers;
    private int numberOfTotalBlocks ;
    private int numberOfPeerServers ;
    private String relativePathname;

    private ISharerGUI tgui;
    public PeerClient(IndexElement[] peerServers, int numberOfTotalBlocks, int numberOfPeerServers , String relativePathname, ISharerGUI tgui){
        this.peerServers = peerServers;
        this.numberOfPeerServers = numberOfPeerServers;
        this.numberOfTotalBlocks = numberOfTotalBlocks;
        this.relativePathname = relativePathname;
        this.tgui = tgui;
    }

    @Override
    public void run(){
        Thread t = new Thread(() -> peerClient(peerServers,numberOfTotalBlocks,numberOfPeerServers,relativePathname ));
        t.start();
    }

    public void peerClient(IndexElement[] peerServers, int numberOfTotalBlocks, int numberOfPeerServers,String relativePathname){
        if (numberOfPeerServers>=numberOfTotalBlocks){
            for (int i = 0;i<numberOfTotalBlocks;i++){
                IndexElement indexPeerServer = peerServers[i];
                int peerServerPort = indexPeerServer.port;
                PeerServer peerServer = new PeerServer(peerServerPort);
                peerServer.start();

                try{
                    String ip = indexPeerServer.ip;
                    int port  = indexPeerServer.port;
                    Socket peerSocket = new Socket(ip,port);

                    BufferedReader p2pServerbufferedReader = getBufferedReader(peerSocket);
                    BufferedWriter p2pServerbufferedWriter = getBufferedWriter(peerSocket);

                    BlockRequest blockRequest = new BlockRequest(indexPeerServer.filename,indexPeerServer.fileDescr.getFileMd5(),i);
                    p2pServerbufferedWriter.write(blockRequest.toString());
                    p2pServerbufferedWriter.newLine();
                    p2pServerbufferedWriter.flush();

                    BlockReply blockReply =null;
                    String jsonStrFromPeerServer = p2pServerbufferedReader.readLine();
                    if(jsonStrFromPeerServer!=null){
                        blockReply = (BlockReply) MessageFactory.deserialize(jsonStrFromPeerServer);
                    }
                    byte[] bytes = Base64.getDecoder().decode(blockReply.bytes);
                    RandomAccessFile downLoadFile = new RandomAccessFile(relativePathname,"rw");
                    FileDescr downLoadFileDescr = new FileDescr(downLoadFile);
                    FileMgr downLoadFileMgr = new FileMgr(relativePathname,downLoadFileDescr );
                    downLoadFileMgr.writeBlock(i,bytes);

                    String jsonStr = p2pServerbufferedReader.readLine();
                    if(jsonStr!=null){
                        Goodbye goodbye= (Goodbye) MessageFactory.deserialize(jsonStr);
                        tgui.logInfo(goodbye.msg);
                    }

                } catch (NoSuchAlgorithmException | IOException | JsonSerializationException e) {
                    throw new RuntimeException(e);
                }
            }

        }else{
            int looptimes = numberOfTotalBlocks/numberOfPeerServers;
            int lastLoopTimes = numberOfTotalBlocks%numberOfPeerServers;
            int countBlocks = 0;
            for (int i = 0;i<numberOfPeerServers;i++){
                for(int j = 0;j<looptimes;j++){
                    IndexElement indexPeerServer = peerServers[i];
                    int peerServerPort = indexPeerServer.port;
                    PeerServer peerServer = new PeerServer(peerServerPort);
                    peerServer.start();
                    try{
                        Socket peerSocket = new Socket(indexPeerServer.ip,indexPeerServer.port);
                        BufferedReader p2pServerbufferedReader = getBufferedReader(peerSocket);
                        BufferedWriter p2pServerbufferedWriter = getBufferedWriter(peerSocket);

                        BlockRequest blockRequest = new BlockRequest(indexPeerServer.filename,indexPeerServer.fileDescr.getFileMd5(),countBlocks);
                        p2pServerbufferedWriter.write(blockRequest.toString());
                        p2pServerbufferedWriter.newLine();
                        p2pServerbufferedWriter.flush();

                        BlockReply blockReply =null;
                        String jsonStrFromPeerServer = p2pServerbufferedReader.readLine();
                        if(jsonStrFromPeerServer!=null){
                            blockReply = (BlockReply) MessageFactory.deserialize(jsonStrFromPeerServer);
                        }
                        byte[] bytes = Base64.getDecoder().decode(blockReply.bytes);
                        RandomAccessFile downLoadFile = new RandomAccessFile(relativePathname,"rw");
                        FileDescr downLoadFileDescr = new FileDescr(downLoadFile);
                        FileMgr downLoadFileMgr = new FileMgr(relativePathname,downLoadFileDescr );
                        downLoadFileMgr.writeBlock(countBlocks,bytes);

                        String jsonStr = p2pServerbufferedReader.readLine();
                        if(jsonStr!=null){
                            Goodbye goodbye= (Goodbye) MessageFactory.deserialize(jsonStr);
                            tgui.logInfo(goodbye.msg);
                        }
                    } catch (NoSuchAlgorithmException e) {
                        throw new RuntimeException(e);
                    } catch (UnknownHostException e) {
                        throw new RuntimeException(e);
                    } catch (JsonSerializationException e) {
                        throw new RuntimeException(e);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    countBlocks = countBlocks+1;
                }

            }

            for (int i = 0;i<lastLoopTimes;i++){
                IndexElement indexPeerServer = peerServers[i];
                int peerServerPort = indexPeerServer.port;
                PeerServer peerServer = new PeerServer(peerServerPort);
                peerServer.start();

                try{
                    Socket peerSocket = new Socket(indexPeerServer.ip,indexPeerServer.port);
                    BufferedReader p2pServerbufferedReader = getBufferedReader(peerSocket);
                    BufferedWriter p2pServerbufferedWriter = getBufferedWriter(peerSocket);

                    BlockRequest blockRequest = new BlockRequest(indexPeerServer.filename,indexPeerServer.fileDescr.getFileMd5(),countBlocks);
                    p2pServerbufferedWriter.write(blockRequest.toString());
                    p2pServerbufferedWriter.newLine();
                    p2pServerbufferedWriter.flush();

                    BlockReply blockReply =null;
                    String jsonStrFromPeerServer = p2pServerbufferedReader.readLine();
                    if(jsonStrFromPeerServer!=null){
                        blockReply = (BlockReply) MessageFactory.deserialize(jsonStrFromPeerServer);
                    }
                    byte[] bytes = Base64.getDecoder().decode(blockReply.bytes);
                    RandomAccessFile downLoadFile = new RandomAccessFile(relativePathname,"rw");
                    FileDescr downLoadFileDescr = new FileDescr(downLoadFile);
                    FileMgr downLoadFileMgr = new FileMgr(relativePathname,downLoadFileDescr );
                    downLoadFileMgr.writeBlock(countBlocks,bytes);

                    String jsonStr = p2pServerbufferedReader.readLine();
                    if(jsonStr!=null){
                        Goodbye goodbye= (Goodbye) MessageFactory.deserialize(jsonStr);
                        tgui.logInfo(goodbye.msg);
                    }

                } catch (NoSuchAlgorithmException | IOException e) {
                    throw new RuntimeException(e);
                } catch (JsonSerializationException e) {
                    throw new RuntimeException(e);
                }
                countBlocks++;
            }
        }
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
