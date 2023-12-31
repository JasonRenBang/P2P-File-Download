package comp90015.idxsrv.peer;

import comp90015.idxsrv.filemgr.BlockUnavailableException;
import comp90015.idxsrv.filemgr.FileDescr;
import comp90015.idxsrv.filemgr.FileMgr;
import comp90015.idxsrv.message.*;
import comp90015.idxsrv.textgui.ISharerGUI;

import javax.net.ServerSocketFactory;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class PeerServer extends Thread{
    public int peerServerPort;


    public PeerServer(int peerServerPort){

        this.peerServerPort = peerServerPort;

    }

    @Override
    public void run(){
        ServerSocketFactory factory = ServerSocketFactory.getDefault();
        try(ServerSocket server = factory.createServerSocket(peerServerPort)){
            while(true) {
                Socket client = server.accept();
                Thread t = new Thread(() -> peerServer(client));
                t.start();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }

    private void peerServer(Socket client){
        try{

            BufferedReader p2pServerbufferedReader = getBufferedReader(client);
            BufferedWriter p2pServerbufferedWriter = getBufferedWriter(client);

            String jsonStr = p2pServerbufferedReader.readLine();
            BlockRequest blockRequest = null;
            if(jsonStr!=null){
                blockRequest = (BlockRequest) MessageFactory.deserialize(jsonStr);
            }

            RandomAccessFile sendRandomAccessFile = new RandomAccessFile(blockRequest.filename,"rw");
            FileDescr sendFileDescr = new FileDescr(sendRandomAccessFile);
            FileMgr sendFileMgr = new FileMgr(blockRequest.filename,sendFileDescr);
            String str64Base = Base64.getEncoder().encodeToString(sendFileMgr.readBlock(blockRequest.blockIdx));
            BlockReply blockReply = new BlockReply(blockRequest.filename, sendFileDescr.getFileMd5(),sendFileDescr.getNumBlocks(),str64Base);
            p2pServerbufferedWriter.write(blockReply.toString());
            p2pServerbufferedWriter.newLine();
            p2pServerbufferedWriter.flush();

            Goodbye goodbye = new Goodbye("Goodbye, the block of "+ peerServerPort+" have sent");
            p2pServerbufferedWriter.write(goodbye.toString());
            p2pServerbufferedWriter.newLine();
            p2pServerbufferedWriter.flush();





        } catch (IOException | JsonSerializationException | NoSuchAlgorithmException | BlockUnavailableException e) {
            e.printStackTrace();
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
