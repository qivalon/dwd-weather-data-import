import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Grid representation necessary to interpret the binary file format.
 */
public class Grid {

    static final double[][] LON, LAT;

    static final int SIZE = 900;

    static final double
        LEFT = -523.4622,
        BOTTOM = -4658.645,
        LON_0 = Math.toRadians(10),
        LAT_0 = Math.toRadians(60),
        RADIUS = 6370.04,
        GRID_SIZE = 1;

    static {
        LON = new double[SIZE][SIZE];
        LAT = new double[SIZE][SIZE];
        for (int i = 0; i < SIZE; i++) {
            for (int j = 0; j < SIZE; j++) {
                double[] lonLat = transform(j, i);
                LON[i][j] = lonLat[0];
                LAT[i][j] = lonLat[1];
            }
        }
    }

    static double[] transform(int i, int j) {
        double x = LEFT + i + GRID_SIZE / 2;
        double y = BOTTOM + j + GRID_SIZE / 2;
        double lon = Math.atan(-x / y) + LON_0;
        double lat = 2
            * Math.atan(RADIUS * (1 + Math.sin(LAT_0)) * Math.sin(lon - LON_0) / x)
            - Math.toRadians(90);
        lon = Math.round(Math.toDegrees(lon) * 1e5) / 1e5;
        lat = Math.round(Math.toDegrees(lat) * 1e5) / 1e5;
        return new double[] { lon, lat };
    }

    // Test
    private static double[][] readGridFile(String file) throws IOException {
        String line;
        BufferedReader reader = new BufferedReader(new FileReader(file));
        double[][] values = new double[900][900];
        int i = 0;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("(?<=\\G.{8})");
            int j = 0;
            for (String part : parts) {
                values[i][j] = Double.parseDouble(part);
                j++;
            }
            i++;
        }
        reader.close();
        return values;
    }

    public static void main(String[] args) throws IOException {
        String dir = "RADOLAN-Raster/Raster-lambda_phi_center";
        double[][] lon = readGridFile(dir + "/lambda_center.txt");
        double[][] lat = readGridFile(dir + "/phi_center.txt");
        double maxDelta = 2e-5;
        for (int i = 0; i < 900; i++) {
            for (int j = 0; j < 900; j++) {
                double diffLon = Math.abs(lon[i][j] - LON[i][j]);
                double diffLat = Math.abs(lat[i][j] - LAT[i][j]);
                assert diffLon <= maxDelta : lon[i][j] + " != " + LON[i][j];
                assert diffLat <= maxDelta : lat[i][j] + " != " + LAT[i][j];
            }
        }
    }
}
