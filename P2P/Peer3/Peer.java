import java.net.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Collectors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Peer {

    //  private static HashSet<String> chunkID = new HashSet<>();
    private static int foChunkIDSize;
    private static int selfPort;
    private static int downPort;
    private static HashSet<String> sChunkID = new HashSet<>();
    private static String filePath;

    Peer(){

    }


    public static void main(String[] args) throws Exception {


        int foPort;   //The server will be listening on this port number

        Socket foSocket;             //socket connect to the file Owner
        Socket downSocket = null;
        ServerSocket upListener;
        ObjectOutputStream out_fo = null;         //stream write to the socket
        ObjectInputStream in_fo = null;          //stream read from the socket
        DataInputStream dis_fo = null;
        FileOutputStream fos_fo = null;


        foPort = Integer.parseInt(args[0]);
        selfPort = Integer.parseInt(args[1]);
        downPort = Integer.parseInt(args[2]);


        try{

            foSocket = new Socket("localhost", foPort);
            System.out.println("Connected to file owner in port 5000");
            //initialize inputStream and outputStream
            in_fo = new ObjectInputStream(foSocket.getInputStream());
            out_fo = new ObjectOutputStream(foSocket.getOutputStream());


            String chunkNum;

            filePath = (String) in_fo.readObject();
            foChunkIDSize = Integer.parseInt((String)in_fo.readObject());
            ArrayList<String> chunkPartList = (ArrayList<String>)in_fo.readObject();
            String[] fileSizeList = (String[]) in_fo.readObject();



            for(int i =0; i< chunkPartList.size();i++) {

                chunkNum = chunkPartList.get(i);
                int fileSize = Integer.parseInt(fileSizeList[i]);
                sChunkID.add(chunkPartList.get(i));
                fos_fo = new FileOutputStream(chunkNum);
                dis_fo = new DataInputStream(foSocket.getInputStream());
                byte[] buffer = new byte[1];
                int read ;
                for (int j =0; j<fileSize; j++ ) {
                    read =  dis_fo.read(buffer, 0, buffer.length);
                    if(read> 0){
                        fos_fo.write(buffer, 0, read);
                    }
                }
                fos_fo.flush();
            }


            foSocket.close();

            // creating ChunkIDFile

            PrintWriter writer = new PrintWriter("ChunkIDList.txt", "UTF-8");

            for(String chunk: sChunkID){
                writer.println(chunk);
            }

            writer.close();


            upListener = new ServerSocket(selfPort);


            boolean connected = false;

            try {


                while(true) {

                    try{

                        if(!connected){
                            downSocket = new Socket("localhost", downPort);
                            new Download(downSocket).start();
                            connected = true;
                        }
                        new Upload(upListener.accept()).start();
                        if(sChunkID.size() == foChunkIDSize){
                            break;
                        }
                    }
                    catch(ConnectException e){
                        System.out.println("Cannot connect to peer");
                        Thread.sleep(1000);
                    }
                    catch(UnknownHostException unknownHost){
                        System.err.println("You are trying to connect to an unknown host!");
                    }
                }

                System.out.println("Enters merging");


            } finally {
                upListener.close();
                downSocket.close();
            }

        }
        catch (ConnectException e) {
            System.err.println("Connection refused. You need to initiate a server first.");
        }
        catch ( ClassNotFoundException e ) {
            System.err.println("Class not found");
        }
        catch(UnknownHostException unknownHost){
            System.err.println("You are trying to connect to an unknown host!");
        }
        catch(IOException ioException){
            ioException.printStackTrace();
        }
        finally{
            //Close connections
            try{
                if(in_fo != null) in_fo.close();
                if(out_fo != null) out_fo.close();
                if(fos_fo != null) fos_fo.close();
                if(dis_fo != null) dis_fo.close();
            }
            catch(IOException ioException){
                ioException.printStackTrace();
            }
        }
    }

    /**
     * A handler thread class.  Handlers are spawned from the listening
     * loop and are responsible for dealing with a single client's requests.
     */
    private static class Download extends Thread {

        private Socket connection;
        private static ObjectOutputStream out_down;         //stream write to the socket
        private static ObjectInputStream in_down;          //stream read from the socket
        private static OutputStream os_down;
        private static InputStream is_down;


        public Download(Socket connection) {
            this.connection = connection;
        }

        public void run() {

            byte[] buffer;
            int read;
            Lock l = new ReentrantLock();
            String randNum;


            try {

                in_down = new ObjectInputStream(connection.getInputStream());
                out_down = new ObjectOutputStream(connection.getOutputStream());


                while(sChunkID.size() != foChunkIDSize){


                    Random rand = new Random();

                    HashSet<String> dNeighborChunks = (HashSet<String>) in_down.readObject();


                    System.out.println("Peer " + selfPort + " has received the chunkID list of peer " + downPort);

                    HashSet<String> missingChunks = dNeighborChunks;


                    missingChunks.removeAll(sChunkID);


                    if(missingChunks.size() == 0){
                        sendMessage("equal sets", out_down);
                        continue;
                    }else{
                        sendMessage("not equal", out_down);
                    }

                    sleep(600);
                    int index = rand.nextInt(missingChunks.size());
                    Iterator<String> iter = missingChunks.iterator();
                    for (int i = 0; i < index; i++) {
                        iter.next();
                    }
                    randNum = iter.next();

                    sendMessage(randNum, out_down);

                    System.out.println("Peer " + selfPort + " has requested chunk " + randNum + " from peer " + downPort);


                    sleep(600);

                    sChunkID.add(randNum);
                    updateFileChunkIDList(randNum );

                    is_down = connection.getInputStream();
                    os_down = new FileOutputStream("./" + randNum );


                    int fileSize = Integer.parseInt((String)in_down.readObject());

                    buffer= new byte[1];

                    for(int i =0; i<fileSize; i++){
                        read = is_down.read(buffer);
                        os_down.write(buffer, 0, read);
                    }

                    os_down.flush();

                    System.out.println("Peer " + downPort + " has received its missing chunk " + randNum + " from peer " + selfPort);

                    if(sChunkID.size() == foChunkIDSize){

                        System.out.println("Done");

                        List<Integer> range = IntStream.rangeClosed(1, sChunkID.size())
                                .boxed().collect(Collectors.toList()) ;

                        List<File> fileList = new ArrayList();

                        for( int r : range){
                            File f = new File("./"+ r);
                            fileList.add(f);
                        }

                        try (FileOutputStream fos_m = new FileOutputStream("./" + filePath);
                             BufferedOutputStream mergingStream = new BufferedOutputStream(fos_m)) {
                            for (File f : fileList) {
                                Files.copy(f.toPath(), mergingStream);
                            }
                        }
                        sleep(10000);
                    }
                /*    else{
                        sendMessage("not done", out_down);
                    } */
                }

            }
            catch(EOFException e)
            {
                e.printStackTrace();
            }
            catch(InterruptedException i){

            }
            catch ( ClassNotFoundException e ) {
                System.err.println("Class not found");
            }
            catch(IOException ioException){
                ioException.printStackTrace();
            }
            finally{
                //Close connections
                try{
                    if(in_down != null) in_down.close();
                    if(out_down != null) out_down.close();
                    if(os_down != null) os_down.close();
                    if(is_down != null) is_down.close();
                }
                catch(IOException ioException){
                    ioException.printStackTrace();
                }
            }

        }

    }


    private static class Upload extends Thread {


        private Socket connection;
        private static ObjectOutputStream out_up;         //stream write to the socket
        private static ObjectInputStream in_up;         //stream read from the socket
        private static OutputStream os_up;
        private static InputStream is_up;



        public Upload(Socket connection) {
            this.connection = connection;
        }

        public void run() {

            Lock l = new ReentrantLock();

            try{

                out_up = new ObjectOutputStream(connection.getOutputStream());
                in_up = new ObjectInputStream(connection.getInputStream());
                HashSet<String> dChunkID ;

                while(true){

                    sleep(600);

                    byte[] buffer;
                    int read;
                    String randNum;

                    dChunkID = getChunkIDFileList();

                    out_up.writeObject(dChunkID);

                    System.out.println("Peer " + selfPort + " has sent its chunkID list to upload peer ");

                    out_up.flush();

                    sleep(600);

                    if(((String)in_up.readObject()).equals("equal set")){
                        continue;
                    }

                    randNum = (String)in_up.readObject();

                    System.out.println("Peer " + selfPort + " has received request for chunk " + randNum + " from upload peer ");

                    sleep(100);

                    File file = new File(randNum);


                    sendMessage(Long.toString(file.length()), out_up);

                    is_up = new FileInputStream(file);             ;
                    os_up = connection.getOutputStream();

                    buffer = new byte[100*1024];

                    while((read = is_up.read(buffer))>0){
                        os_up.write(buffer, 0, read);
                    }
                    System.out.println("Peer " + selfPort + " has sent chunk " + randNum + " to upload peer" );
                    os_up.flush();

                }
            }
            catch(EOFException e){
                e.printStackTrace();

            }
            catch(InterruptedException i){
                i.printStackTrace();
            }
            catch(IOException ie){
                ie.printStackTrace();
            }
            catch(ClassNotFoundException c){
                System.err.println("Class not found");
            }
            finally{
                //Close connections
                try{
                    if(in_up != null) in_up.close();
                    if(out_up != null) out_up.close();
                    if(os_up != null) os_up.close();
                    if(is_up != null) is_up.close();
                }
                catch(IOException ioException){
                    ioException.printStackTrace();
                }
            }
        }
    }

    //send a message to the output stream
    public static void sendMessage(String msg, ObjectOutputStream out)
    {
        try{
            out.writeObject(msg);
            out.flush();
        }
        catch(IOException ioException){
            ioException.printStackTrace();
        }
    }


    public static HashSet<String> getChunkIDFileList(){

        HashSet<String> chunkID= new HashSet<>();

        File file = new File("ChunkIDList.txt");

        try{
            Scanner sc = new Scanner(file);
            while (sc.hasNextLine())
                chunkID.add(sc.nextLine());
        }
        catch(FileNotFoundException f){
            System.out.println("File not found");
        }
        return chunkID;
    }



    public static void updateFileChunkIDList(String newChunk){

        try {
            Files.write(Paths.get("ChunkIDList.txt"), (newChunk +"\n" ).getBytes(), StandardOpenOption.APPEND);
        }catch (IOException e) {
            System.out.println("File not Found");
        }
    }
}
