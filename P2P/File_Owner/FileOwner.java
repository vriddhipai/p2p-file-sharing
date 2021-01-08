import java.net.*;
import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;

public class FileOwner {

    private static int sPort;   //The server will be listening on this port number
    private static HashSet<String> chunkID = new HashSet();
    private static String filePath;

    FileOwner(){

    }


    public static void main(String[] args) throws Exception {

        sPort = Integer.parseInt(args[0]);


        System.out.println("File Owner server is running.");
        System.out.println("Please enter the path of the file to be uploaded to server: ");


        Scanner scanner = new Scanner(System.in);
        filePath = scanner.nextLine();
        File f = new File(filePath);


        int sizeOfFiles = 1024 * 100;// 100 KB
        byte[] buffer = new byte[sizeOfFiles];
        int partCounter = 1;


        try (FileInputStream fis = new FileInputStream(f);
             BufferedInputStream bis = new BufferedInputStream(fis)) {

            int bytesAmount = 0;
            while ((bytesAmount = bis.read(buffer)) > 0) {
                //write each chunk of data into separate file with different number in name
                String filePartName = Integer.toString(partCounter++);
                chunkID.add(filePartName);
                File newFile = new File(f.getParent(), filePartName);
                try (FileOutputStream out = new FileOutputStream(newFile)) {
                    out.write(buffer, 0, bytesAmount);
                }
            }
        }

        Iterator<String> it = chunkID.iterator();
        int peerNumChunks= chunkID.size()/5;
        ArrayList<String>[] chunkParts = new ArrayList[5];

        for (int i = 0; i < 5; i++) {
            chunkParts[i] = new ArrayList<>();
        }

        int count =0;
        for(int i=0; i<5; i++){
            for(int j =0; j<peerNumChunks; j++){
                chunkParts[i].add(it.next());
                count++;
            }
        }

        if(count < chunkID.size()){
            int diff = chunkID.size() - count;
            for(int i =0; i<diff; i++){
                chunkParts[0].add(it.next());
            }
        }

        ServerSocket listener = new ServerSocket(sPort);

        int peerNum = 1;
        try {
            while(true) {
                new Handler(listener.accept(),peerNum, chunkParts).start();
                System.out.println("Peer "  + peerNum + " is connected!");
                peerNum++;
            }
        } finally {
            listener.close();
        }
    }

    /**
     * A handler thread class.  Handlers are spawned from the listening
     * loop and are responsible for dealing with a single client's requests.
     */
    private static class Handler extends Thread {
        private Socket connection;
        private ObjectInputStream in;	//stream read from the socket
        private ObjectOutputStream out;    //stream write to the socket
        private InputStream is;
        private DataOutputStream dos;
        private FileInputStream fis;
        private int peerNum;		//The index number of the client
        private ArrayList<String>[] chunkParts;


        public Handler(Socket connection, int peerNum, ArrayList<String>[] chunkParts) {
            this.connection = connection;
            this.peerNum = peerNum;
            this.chunkParts = chunkParts;
        }

        public void run() {


            try{
                //initialize Input and Output streams


                out = new ObjectOutputStream(connection.getOutputStream());
                in = new ObjectInputStream(connection.getInputStream());


                sendMessage(filePath);

                sendMessage(Integer.toString(chunkID.size()));

                String[] fileSizeList = new String[chunkParts[peerNum-1].size()];

                out.writeObject(chunkParts[peerNum-1]);


                for(int i =0; i< chunkParts[peerNum-1].size(); i++){
                    File file = new File(chunkParts[peerNum-1].get(i));
                    fileSizeList[i] = Long.toString(file.length());
                }

                out.writeObject(fileSizeList);

                out.flush();


                for(int i =0; i< chunkParts[peerNum-1].size(); i++){

                    File file = new File(chunkParts[peerNum-1].get(i));
                    fis = new FileInputStream(file);
                    dos = new DataOutputStream(connection.getOutputStream());
                    byte[] buffer = new byte[100*1024];

                    while (fis.read(buffer) > 0) {
                        dos.write(buffer);
                    }
                    dos.flush();
                }

            }
            catch(IOException ioException){
                System.out.println("Disconnect with Client " + peerNum);
            }
            finally{
                //Close connections
                try{
                    if(in != null)in.close();
                    if(out != null)out.close();
                    if(dos != null)dos.close();
                    if(fis != null)fis.close();
                    connection.close();
                }
                catch(IOException ioException){
                    System.out.println("Disconnect with peer " + peerNum);
                }
            }

        }

        //send a message to the output stream
        public void sendMessage(String msg)
        {
            try{
                out.writeObject(msg);
                out.flush();
            }
            catch(IOException ioException){
                ioException.printStackTrace();
            }
        }

    }

}
