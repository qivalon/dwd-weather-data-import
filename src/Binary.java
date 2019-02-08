import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class Binary {

    private static final String FILE_REGEX = "raa01-rw_10000-(\\d{10})-dwd---bin";

    private static final String FILE_DATE_FORMAT = "yyMMddHHmm";

    private static final byte ETX = 0x03;

    static byte[] readBinaryFile(File file) throws IOException {
        InputStream in = file.getName().endsWith(".gz")
            ? new GZIPInputStream(new FileInputStream(file))
            : new FileInputStream(file);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int read = 0;
        byte[] buffer = new byte[4096];
        while ((read = in.read(buffer)) != -1)
            out.write(buffer, 0, read);
        in.close();
        byte[] bytes = out.toByteArray();
        out.close();
        return bytes;
    }

    static String getHeader(byte[] bytes) {
        for (int i = 0; i < bytes.length; i++)
            if (bytes[i] == ETX)
                return new String(bytes, 0, i - 1);
        return null;
    }

    static int[][] getValues(byte[] bytes, int offset) {
        int[][] values = new int[Grid.SIZE][Grid.SIZE];
        int x = 0;
        int y = Grid.SIZE - 1;
        for (int i = offset; i < bytes.length; i += 2) {
            int bits = bytes[i] << 8 & 0xff00 | bytes[i - 1] & 0xff;
            int type = bits >> 12;
            int value = bits & 0xfff;
            if (type == 0x02)
                value = -1;
            values[y][x] = value;
            if (++x == Grid.SIZE) {
                x = 0;
                y--;
            }
        }
        return values;
    }

    // Test
    static boolean isEquals(int[][] values, int[][] values2) {
        for (int i = 0; i < values.length; i++)
            for (int j = 0; j < values.length; j++)
                if (values[i][j] != values2[i][j])
                    return false;
        return true;
    }

    // Test
    private static int[][] readAsciiFile(String dir, Date date) throws IOException {
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmm");
        File file = new File(dir + "/RW_" + dateFormat.format(date) + ".asc");
        String line;
        BufferedReader reader = new BufferedReader(new FileReader(file));
        boolean start = false;
        int[][] values = new int[Grid.SIZE][Grid.SIZE];
        int i = 0;
        while ((line = reader.readLine()) != null) {
            if (!start) {
                start = line.startsWith("NODATA");
                continue;
            }
            String[] parts = line.split(" ");
            for (int j = 0; j < parts.length; j++)
                values[i][j] = Integer.parseInt(parts[j]);
            i++;
        }
        reader.close();
        return values;
    }

    public static void main(String[] args) throws IOException, ParseException {
        String binaryDir = "201601/bin";
        String asciiDir = "201601/asc";
        
        DateFormat dateFormat = new SimpleDateFormat(FILE_DATE_FORMAT);
        Pattern pattern = Pattern.compile(FILE_REGEX);
        
        File[] files = new File(binaryDir).listFiles();
        for (File file : files) {
            String fileName = file.getName();
            Matcher matcher = pattern.matcher(fileName);
            matcher.find();
            Date date = dateFormat.parse(matcher.group(1));
            
            byte[] bytes = readBinaryFile(file);
            String header = getHeader(bytes);
            assert header != null : "header is null";
            System.out.println(header);
            
            int[][] values = getValues(bytes, header.length() + 3);
            int[][] values2 = readAsciiFile(asciiDir, date); 
            assert isEquals(values, values2);
        }
    }

}
