import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class ImportWeatherData {

    static class Station {
        static final String CSV_HEADER = 
            "stationId;from;to;lon;lat;alt;name;state";    
        int id;
        Date from;
        Date to;
        int alt;
        double lon;
        double lat;
        String name;
        String state;
    }

    static class AirTemperature {
        static final String CSV_HEADER = 
            "stationId;measurementTime;airTemperature;relativeHumidity";
            //"stationId;measurementTime;quality;airAirTemperature;relativeHumidity";
        int stationId;
        Date measurementTime;
        int quality;
        double airTemperature;
        int relativeHumidity;
    }

    static class Wind {
        static final String CSV_HEADER = 
            "stationId;measurementTime;meanWindSpeed;meanWindDirection";
            //"stationId;measurementTime;quality;meanWindSpeed;meanWindDirection";
        int stationId;
        Date measurementTime;
        int quality;
        double meanWindSpeed;
        int meanWindDirection;
    }

    static class Precipitation {
        static final String CSV_HEADER = 
            "stationId;measurementTime;height;form";
            //"stationId;measurementTime;quality;height;hasFallen;form";
        int stationId;
        Date measurementTime;
        int quality;
        double height;
        boolean hasFallen;
        int form;
    }
    
    static class Pressure {
    	static final String CSV_HEADER =
    			"stationId;measurementTime;pressureNN;pressureStationHeight";
    			//"stationId;measurementTime;quality;pressureNN;pressureStationHeight";
    	int stationId;
    	Date measurementTime;
    	int quality;
    	double pressureNN;
    	double pressureStationHeight;
    }

    static final String DATE_FORMAT = "yyyyMMdd";
    static final String DATE_TIME_FORMAT = "yyyyMMddHH";
    static final String CSV_DATE_FORMAT = "yyyy-MM-dd";
    static final String CSV_DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm";
    static final int MISSING_VALUE = -999;

    static final String REGEX_HISTORICAL = 
        "stundenwerte_[A-Za-b0-9]{2}_(\\d{5})_(\\d{8})_(\\d{8})_hist.zip";    
    static final String REGEX_CURRENT =
        "stundenwerte_[A-Z]{2}_\\d+_akt.zip";

    static void readStations(Map<Integer, Station> stations, String file)
        throws IOException, ParseException {

        BufferedReader reader = new BufferedReader(new InputStreamReader(
            new FileInputStream(file), "ISO-8859-1"));
        DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
        String line;
        reader.readLine();
        reader.readLine();

        while ((line = reader.readLine()) != null) {
            Station station = new Station();
            station.id = Integer.parseInt(line.substring(0, 6).trim());
            station.from = dateFormat.parse(line.substring(6, 15).trim());
            station.to = dateFormat.parse(line.substring(15, 24).trim());
            station.alt = Integer.parseInt(line.substring(24, 39).trim());
            station.lat = Double.parseDouble(line.substring(39, 51).trim());
            station.lon = Double.parseDouble(line.substring(51, 61).trim());
            station.name = line.substring(61, 102).trim();
            station.state = line.substring(102).trim();
            stations.put(station.id, station);
        }
        reader.close();
    }

    static void writeStations(Map<Integer, Station> stations, String outputFileName)
        throws IOException {

        BufferedWriter writer = new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream(outputFileName), "UTF-8"));
        writer.write(Station.CSV_HEADER);
        writer.newLine();
        DateFormat dateFormat = new SimpleDateFormat(CSV_DATE_FORMAT);

        for (Station station : stations.values()) {
            writer.write(station.id
                + ";" + dateFormat.format(station.from)
                + ";" + dateFormat.format(station.to)
                + ";" + station.lon
                + ";" + station.lat
                + ";" + station.alt
                + ";" + station.name
                + ";" + station.state);
            writer.newLine();
        }
        writer.close();
    }

    static List<Pressure> readPressures(File file, Date from, Date to,
    		Map<Integer, Station> stations) throws IOException, ParseException {
    	if (!file.getName().endsWith("zip"))
    		return null;
    	
    	ZipFile zipFile = new ZipFile(file);
    	BufferedReader reader = readFromZip(zipFile, from, to);
    	if (reader == null)
    		return null;

    	DateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT);
        dateTimeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    	List<Pressure> pressures = new ArrayList<>();
    	String line = reader.readLine();

    	while ((line = reader.readLine()) != null) {
			String[] parts = line.split(";");
			Date measurementTime = dateTimeFormat.parse(parts[1]);
			if (measurementTime.before(from) || measurementTime.after(to)) 
				continue;
			Pressure pressure = new Pressure();
			pressure.stationId = Integer.parseInt(parts[0].trim());
			if (!stations.containsKey(pressure.stationId))
				continue;
			pressure.measurementTime = measurementTime;
			pressure.quality = Integer.parseInt(parts[2].trim());
			pressure.pressureNN = Double.parseDouble(parts[3].trim());
			if (pressure.pressureNN == MISSING_VALUE)
				continue;
			pressure.pressureStationHeight = Double.parseDouble(parts[4].trim());
			if (pressure.pressureStationHeight == MISSING_VALUE)
				pressure.pressureStationHeight = -1;
			pressures.add(pressure);
		}
    	reader.close();
    	zipFile.close();
    	
    	return pressures;
    }

    static List<AirTemperature> readAirTemperatures(File file, Date from, Date to,
        Map<Integer, Station> stations) throws IOException, ParseException {
        
        if (!file.getName().endsWith("zip"))
            return null;
        
        ZipFile zipFile = new ZipFile(file);
        BufferedReader reader = readFromZip(zipFile, from ,to);
        if (reader == null)
            return null;
        
        DateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT);
        dateTimeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<AirTemperature> airTemperatures = new ArrayList<AirTemperature>();
        String line = reader.readLine();

        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(";");
            Date measurementTime = dateTimeFormat.parse(parts[1]);
            if (measurementTime.before(from) || measurementTime.after(to))
                continue;
            AirTemperature airTemperature = new AirTemperature();
            airTemperature.stationId = Integer.parseInt(parts[0].trim());
            if (!stations.containsKey(airTemperature.stationId))
                continue;
            airTemperature.measurementTime = measurementTime;
            airTemperature.quality = Integer.parseInt(parts[2].trim());
            airTemperature.airTemperature = Double.parseDouble(parts[3].trim());
            airTemperature.relativeHumidity = (int) Double.parseDouble(parts[4].trim());
            if (airTemperature.airTemperature == MISSING_VALUE)
                continue;
            if (airTemperature.relativeHumidity == MISSING_VALUE)
                airTemperature.relativeHumidity = -1;        
            airTemperatures.add(airTemperature);
        }

        reader.close();
        zipFile.close();
        return airTemperatures;
    }

    static void writePressures(List<Pressure> pressures, String filename) throws IOException {
    	File file = new File(filename);
    	boolean firstLine = !file.exists();
    	BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
    			new FileOutputStream(file, true), "UTF-8"));
    	if (firstLine) {
    		writer.write(Pressure.CSV_HEADER);
    		writer.newLine();
    	}
    	DateFormat dateFormat = new SimpleDateFormat(CSV_DATE_TIME_FORMAT);
    	
    	for (Pressure pressure : pressures) {
    		writer.write(pressure.stationId + ";" +
    				dateFormat.format(pressure.measurementTime) + ";" +
    				//pressure.quality + ";" +
    				pressure.pressureNN + ";" +
    				pressure.pressureStationHeight);
    		writer.newLine();
    	}
    	writer.close();
    }

    static void writeAirTemperatures(List<AirTemperature> airTemperatures, String fileName)
        throws IOException {

        File file = new File(fileName);
        boolean firstLine = !file.exists();
        BufferedWriter writer = new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream(file, true), "UTF-8"));
        if (firstLine) {
            writer.write(AirTemperature.CSV_HEADER);
            writer.newLine();
        }
        DateFormat dateFormat = new SimpleDateFormat(CSV_DATE_TIME_FORMAT);

        for (AirTemperature airTemperature : airTemperatures) {
            writer.write(airTemperature.stationId
                + ";" + dateFormat.format(airTemperature.measurementTime)
                //+ ";" + airTemperature.quality 
                + ";" + airTemperature.airTemperature
                + ";" + airTemperature.relativeHumidity);
            writer.newLine();
        }
        writer.close();
    }

    static List<Wind> readWinds(File file, Date from, Date to,
        Map<Integer, Station> stations) throws IOException, ParseException {

        if (!file.getName().endsWith("zip"))
            return null;
        ZipFile zipFile = new ZipFile(file);
        BufferedReader reader = readFromZip(zipFile, from ,to);
        if (reader == null)
            return null;

        DateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT);
        dateTimeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Wind> winds = new ArrayList<Wind>();
        String line = reader.readLine();

        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(";");
            Date measurementTime = dateTimeFormat.parse(parts[1]);
            if (measurementTime.before(from) || measurementTime.after(to))
                continue;
            Wind wind = new Wind();
            wind.stationId = Integer.parseInt(parts[0].trim());
            if (!stations.containsKey(wind.stationId))
                continue;
            wind.measurementTime = measurementTime;
            wind.quality = Integer.parseInt(parts[2].trim());
            wind.meanWindSpeed = Double.parseDouble(parts[3].trim());
            wind.meanWindDirection = Integer.parseInt(parts[4].trim());
            if (wind.meanWindSpeed == MISSING_VALUE)
                continue;
            if (wind.meanWindDirection == MISSING_VALUE)
                wind.meanWindDirection = -1;
            winds.add(wind);
        }

        reader.close();
        zipFile.close();
        return winds;
    }

    static void writeWinds(List<Wind> winds, String fileName) throws IOException {
        File file = new File(fileName);
        boolean firstLine = !file.exists();
        BufferedWriter writer = new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream(file, true), "UTF-8"));
        if (firstLine) {
            writer.write(Wind.CSV_HEADER);
            writer.newLine();
        }

        DateFormat dateFormat = new SimpleDateFormat(CSV_DATE_TIME_FORMAT);
        for (Wind w : winds) {
            writer.write(w.stationId
                + ";" + dateFormat.format(w.measurementTime) 
                //+ ";" + w.quality
                + ";" + w.meanWindSpeed
                + ";" + w.meanWindDirection);
            writer.newLine();
        }
        writer.close();
    }

    static List<Precipitation> readPrecipitations(File file, Date from, Date to,
        Map<Integer, Station> stations) throws IOException, ParseException { 

        if (!file.getName().endsWith("zip"))
            return null;

        ZipFile zipFile = new ZipFile(file);
        BufferedReader reader = readFromZip(zipFile, from ,to);
        if (reader == null)
            return null;

        DateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT);
        dateTimeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Precipitation> precipitations = new ArrayList<Precipitation>();
        String line = reader.readLine();

        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(";");
            Date measurementTime = dateTimeFormat.parse(parts[1]);
            if (measurementTime.before(from) || measurementTime.after(to))
                continue;
            Precipitation precipitation = new Precipitation();
            precipitation.stationId = Integer.parseInt(parts[0].trim());
            if (!stations.containsKey(precipitation.stationId))
                continue;
            precipitation.measurementTime = measurementTime;
            precipitation.quality = Integer.parseInt(parts[2].trim());
            precipitation.height = Double.parseDouble(parts[3].trim());
            precipitation.hasFallen = parts[4].trim().equals("1");
            precipitation.form = Integer.parseInt(parts[5].trim());
            if (precipitation.height == MISSING_VALUE || !precipitation.hasFallen)
                continue;
            if (precipitation.form == MISSING_VALUE)
                precipitation.form = -1;
            precipitations.add(precipitation);
        }

        reader.close();
        zipFile.close();
        return precipitations;
    }
    
    static void writePrecipitations(List<Precipitation> precipitations, String fileName)
        throws IOException {

        File file = new File(fileName);
        boolean firstLine = !file.exists();
        BufferedWriter writer = new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream(file, true), "UTF-8"));
        if (firstLine) {
            writer.write(Precipitation.CSV_HEADER);
            writer.newLine();
        }

        DateFormat dateFormat = new SimpleDateFormat(CSV_DATE_TIME_FORMAT);
        for (Precipitation precipitation : precipitations) {
            writer.write(precipitation.stationId
                + ";" + dateFormat.format(precipitation.measurementTime) 
                //+ ";" + precipitation.quality
                + ";" + precipitation.height
                //+ ";" + precipitation.hasFallen
                + ";" + precipitation.form);
            writer.newLine();
        }
        writer.close();
    }

    private static BufferedReader readFromZip(ZipFile zipFile, Date from, Date to)
        throws ParseException, IOException {
 
        Pattern patternHistorical = Pattern.compile(REGEX_HISTORICAL);
        Matcher matcherHistorical = patternHistorical.matcher(zipFile.getName());
        boolean isHistoricalFile = matcherHistorical.find();
        
        Pattern patternCurrent = Pattern.compile(REGEX_CURRENT);
        Matcher matcherCurrent = patternCurrent.matcher(zipFile.getName());
        boolean isCurrentFile = matcherCurrent.find();
        
        if (!isHistoricalFile && !isCurrentFile)
            return null;
        
        DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
        if (isHistoricalFile) {
            if (dateFormat.parse(matcherHistorical.group(2)).after(to) ||
                dateFormat.parse(matcherHistorical.group(3)).before(from))
                return null;
        }
        
        Enumeration<? extends ZipEntry> entries = zipFile.entries();
        BufferedReader reader = null;
        while (entries.hasMoreElements()) {
            ZipEntry entry = entries.nextElement();
            if (entry.getName().startsWith("produkt_")) {
                reader = new BufferedReader(
                    new InputStreamReader(zipFile.getInputStream(entry)));
                break;
            }
        }
        
        return reader;
    }
    
    public static void main(String[] args) throws IOException, ParseException {
    	
    	System.out.println(new File("").getAbsolutePath());
    	
        Calendar cal = Calendar.getInstance();
        cal.set(2012, 0, 1, 0, 0);
        Date from =  cal.getTime();
        cal.set(2018, 0, 1, 0, 0);
        Date to = cal.getTime();

        cal = Calendar.getInstance();
        cal.set(cal.get(Calendar.YEAR) - 1, 11, 31, 23, 59);
        Date endOfLastYear = cal.getTime();

        // Input files
        String dir = "dwd";
        String airTemperatureDir = dir + "/air_temperature";
        String precipitationDir = dir + "/precipitation";
        String pressureDir = dir + "/pressure";
        String windDir = dir + "/wind";

        // Output files
        String stationFile = "weatherstation.csv";
        String airTemperatureFile = "air_temperature.csv";
        String windFile = "wind.csv";
        String precipitationFile = "precipitation.csv";
        String pressureFile = "pressure.csv";    

        // Remove existing output files
        new File(stationFile).delete();
        new File(airTemperatureFile).delete();
        new File(windFile).delete();
        new File(precipitationFile).delete();
        new File(pressureFile).delete();

        // Read in all stations
        Map<Integer, Station> stations = new TreeMap<Integer, Station>();
        readStations(stations, pressureDir + "/historical/"
            + "P0_Stundenwerte_Beschreibung_Stationen.txt");
        readStations(stations, airTemperatureDir + "/historical/"
            + "TU_Stundenwerte_Beschreibung_Stationen.txt");
        readStations(stations, windDir + "/historical/"
            + "FF_Stundenwerte_Beschreibung_Stationen.txt");
        readStations(stations, precipitationDir + "/historical/"
            + "RR_Stundenwerte_Beschreibung_Stationen.txt");
        writeStations(stations, stationFile);

        new Thread() {
            public void run() {
                try {
                    File[] files = new File(airTemperatureDir + "/historical").listFiles();
                    int count = 0;
                    if (files != null) {
                    	for (File file : files) {
                            System.out.println(++count + "/" + files.length);
                            List<AirTemperature> airTemperatures =
                                readAirTemperatures(file, from, to, stations);
                            if (airTemperatures != null && airTemperatures.size() > 0)
                                writeAirTemperatures(airTemperatures, airTemperatureFile);
                        }
                    }
                    files = new File(airTemperatureDir + "/recent").listFiles();
                    count = 0;
                    if (files == null) {
                    	return;
                    }
                    for (File file : files) {
                        System.out.println(++count + "/" + files.length);
                        List<AirTemperature> airTemperatures = readAirTemperatures(file,
                            from.before(endOfLastYear) ? endOfLastYear : from, to,
                            stations);
                        if (airTemperatures != null && airTemperatures.size() > 0)
                            writeAirTemperatures(airTemperatures, airTemperatureFile);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        new Thread() {
            public void run() {
                try {
                    File[] files = new File(windDir + "/historical").listFiles();
                    int count = 0;    
                    if (files != null) {
	                    for (File file : files) {
	                        System.out.println(++count + "/" + files.length);
	                        List<Wind> winds = readWinds(file, from, to, stations);
	                        if (winds != null && winds.size() > 0)
	                            writeWinds(winds, windFile);
	                    }
                    }
                    files = new File(windDir + "/recent").listFiles();
                    count = 0;  
                    if (files == null) {
                    	return;
                    }
                    for (File file : files) {
                        System.out.println(++count + "/" + files.length);
                        List<Wind> winds = readWinds(file,
                            from.before(endOfLastYear) ? endOfLastYear : from, to,
                            stations);
                        if (winds != null && winds.size() > 0)
                            writeWinds(winds, windFile);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        new Thread() {
            public void run() {
                try {
                    File[] files = new File(precipitationDir + "/historical").listFiles();
                    int count = 0;
                    if (files != null) {
                    	for (File file : files) {
                            System.out.println(++count + "/" + files.length);
                            List<Precipitation> precipitations =
                                readPrecipitations(file, from, to, stations);
                            if (precipitations != null && precipitations.size() > 0)
                                writePrecipitations(precipitations, precipitationFile);
                        }
                    }
                    
                    files = new File(precipitationDir + "/recent").listFiles();
                    count = 0;
                    if (files == null) {
                    	return;
                    }
                    for (File file : files) {
                        System.out.println(++count + "/" + files.length);
                        List<Precipitation> precipitations = readPrecipitations(file,
                            from.before(endOfLastYear) ? endOfLastYear : from, to,
                            stations);
                        if (precipitations != null && precipitations.size() > 0)
                            writePrecipitations(precipitations, precipitationFile);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        
        new Thread() {
            public void run() {
                try {
                    File[] files = new File(pressureDir + "/historical").listFiles();
                    int count = 0;
                    if (files != null) {
                    	for (File file : files) {
                            System.out.println(++count + "/" + files.length);
                            List<Pressure> pressures =
                                readPressures(file, from, to, stations);
                            if (pressures != null && pressures.size() > 0)
                                writePressures(pressures, pressureFile);
                        }
                    }
                    files = new File(pressureDir + "/recent").listFiles();
                    count = 0;
                    if (files == null) {
                    	return;
                    }
                    for (File file : files) {
                        System.out.println(++count + "/" + files.length);
                        List<Pressure> pressures = readPressures(file,
                            from.before(endOfLastYear) ? endOfLastYear : from, to,
                            stations);
                        if (pressures != null && pressures.size() > 0)
                            writePressures(pressures, pressureFile);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }.start();
    }
}
