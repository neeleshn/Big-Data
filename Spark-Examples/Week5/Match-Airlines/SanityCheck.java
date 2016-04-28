
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SanityCheck {
	static long half_hour = 1800000;
	static long six_hours = 21600000;

	public static String dateStringFormat(String dateString){
		dateString = "0000" + dateString;
		return dateString.substring(dateString.length()-4);
	}

	public static boolean isConnection(String f, String g) throws ParseException{
		
		String[] fArray= f.split("\t");
		String[] gArray= g.split("\t");

		String sch_arr_string = fArray[0] +" "+ dateStringFormat(fArray[1]);
		String sch_dep_string = gArray[0] +" "+ dateStringFormat(gArray[1]);

		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hhmm");
		Date sch_arr = formatter.parse(sch_arr_string);
		Date sch_dep = formatter.parse(sch_dep_string);
		
		long sch_time_diff = sch_dep.getTime() - sch_arr.getTime();
		
		if(sch_time_diff <= six_hours && sch_time_diff >= half_hour){
			return true;
		}
		return false;
	}

	public static boolean isConnectionMissed(String f, String g) throws ParseException{
		
		String[] fArray= f.split("\t");
		String[] gArray= g.split("\t");

		String sch_arr_string = fArray[0] +" "+ dateStringFormat(fArray[1]);
		String actual_arr_string = fArray[0] +" "+ dateStringFormat(fArray[2]);
		String sch_dep_string = gArray[0] +" "+ dateStringFormat(gArray[1]);
		String actual_dep_string = gArray[0] +	" "+ dateStringFormat(gArray[2]);

		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hhmm");
		Date sch_arr = formatter.parse(sch_arr_string);
		Date actual_arr = formatter.parse(actual_arr_string);
		Date sch_dep = formatter.parse(sch_dep_string);
		Date actual_dep = formatter.parse(actual_dep_string);
		
		long sch_time_diff = sch_dep.getTime() - sch_arr.getTime();
		long actual_time_diff = actual_dep.getTime() - actual_arr.getTime();
		
		if(sch_time_diff <= six_hours && sch_time_diff >= half_hour){
			
			if(actual_time_diff < half_hour) {
				return true;
			}	
		}
		return false;
	}

    public static String[] parseCSVLine(String line) {
        List<String> values = new ArrayList<String>();
        StringBuffer sb = new StringBuffer();
        boolean inQuote = false;
        char curChar;
        for (int i = 0; i < line.length(); i++) {
            curChar = line.charAt(i);
            if (inQuote) {
                if (curChar == '"') {
                    inQuote = false;
                } else {
                    sb.append(curChar);
                }
            } else {
                if (curChar == '"') {
                    inQuote = true;
                } else if (curChar == ',') {
                    values.add(sb.toString());
                    sb = new StringBuffer();
                } else {
                    sb.append(curChar);
                }
            }
        }
        values.add(sb.toString());
        return values.toArray(new String[1]);
    }

	private static int hhmmDiff (String arr, String dep){
		if(arr.length()==3){
			arr='0'+arr;
		}
		if(dep.length()==3){
			dep='0'+dep;
		}
		if (Integer.parseInt(arr) > Integer.parseInt(dep)){
			return (Integer.parseInt(arr.substring(0, 2)) - Integer.parseInt(dep.substring(0, 2))) * 60 +
					(Integer.parseInt(arr.substring(2, 4)) - Integer.parseInt(dep.substring(2, 4)));
		} else {
			return (Integer.parseInt(arr.substring(0, 2)) - Integer.parseInt(dep.substring(0, 2)) + 24) * 60 +
					(Integer.parseInt(arr.substring(2, 4)) - Integer.parseInt(dep.substring(2, 4)));
		}
	}
	
	
    public static boolean validRow(String[] row){
    	try{
			// CRSArrTime and CRSDepTime should not be zero
			// timeZone % 60 should be 0

			String crsArrTime = row[40];
			String crsDepTime = row[29];
			int crsElapsedTime = Integer.parseInt(row[50]);
			
			if (crsArrTime.equals("") || crsDepTime.equals("") || crsArrTime.equals("0") || crsDepTime.equals("0")){
				return false;
			}
			
			int crsDiff = hhmmDiff(crsArrTime, crsDepTime);
			
			int timeZone = crsDiff - crsElapsedTime;
			
			if ((timeZone % 60) != 0){
				return false;
			}
			
			
			// AirportID,  AirportSeqID, CityMarketID, StateFips, Wac should be larger than 0
			
			int originAirportId = Integer.parseInt(row[11]);
			if (originAirportId <= 0) return false;
			
			int originAirportSeqId = Integer.parseInt(row[12]);
			if (originAirportSeqId <= 0) return false;
			
			int originCityMarketID = Integer.parseInt(row[13]);
			if (originCityMarketID <= 0) return false;
			
			int originStateFips = Integer.parseInt(row[17]);
			if (originStateFips <= 0) return false;
			
			int originWac = Integer.parseInt(row[19]);
			if (originWac <= 0) return false;
			
			int destAirportId = Integer.parseInt(row[20]);
			if (destAirportId <= 0) return false;
			
			int destAirportSeqId = Integer.parseInt(row[21]);
			if (destAirportSeqId <= 0) return false;
			
			int destCityMarketId = Integer.parseInt(row[22]);
			if (destCityMarketId <= 0) return false;
			
			int destStateFips = Integer.parseInt(row[26]);
			if (destStateFips <= 0) return false;
			
			int destWac = Integer.parseInt(row[28]);
			if (destWac <= 0) return false;
			
			
			// Origin, Destination,  CityName, State, StateName should not be empty

			String origin = row[14];
			if (origin.equals("")) return false;
			
			String originCityName = row[15];
			if (originCityName.equals("")) return false;
			
			String originStateAbr = row[16];
			if (originStateAbr.equals("")) return false;
			
			String originStateName = row[18];
			if (originStateName.equals("")) return false;
			
			String dest = row[23];		
			if (dest.equals("")) return false;
			
			String destCityName = row[24];
			if (destCityName.equals("")) return false;
			
			String destStateAbr = row[25];
			if (destStateAbr.equals("")) return false;
			
			String destStateName = row[27];
			if (destStateName.equals("")) return false;
			
			
			// For flights that are not Cancelled:
			
			int cancelled = Integer.parseInt(row[47]);		
			
			if (cancelled != 1){			
				
				String arrTime = row[41];
				String depTime = row[30];
				int actualElapsedTime = Integer.parseInt(row[51]);
				
				int actualDiff = hhmmDiff(arrTime, depTime);
				
				int actualTimeZone = actualDiff - actualElapsedTime;
				
				crsDiff = hhmmDiff(crsArrTime, crsDepTime);
				int newtimeZone = crsDiff - crsElapsedTime;

				if (actualTimeZone != newtimeZone) {
					return false;
				}
				
				
				// if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
				// if ArrDelay < 0 then ArrDelayMinutes should be zero
				// if ArrDelayMinutes >= 15 then ArrDel15 should be false
				
				float arrDelay = Float.parseFloat(row[42]);
				float arrDelayMinutes = Float.parseFloat(row[43]);
				float arrDel15 = Float.parseFloat(row[44]);
				
				if (arrDelay > 0.0){
					if (arrDelay != arrDelayMinutes) {
						return false;
					}
				}

				if (arrDelay < 0.0){
					if (arrDelayMinutes != 0) {
						return false;
					}
				}
				
				if (arrDelayMinutes > 15.0){
					if (arrDel15 != 1) {
						return false;
					}
				}
			}
			
			//Extra checks for Assignment 5/6
			String carrier = row[6];
			if (carrier.length()!=2){
				return false;
			}
			
			String date = row[5];
			if(date.length()!=10){
				return false;
			}
			
			int actArrLen = row[41].length();
			int actDepLen = row[30].length();
			
			if (!(actArrLen == 3 || actArrLen == 4)) {
				return false;
			}
			if (!(actDepLen == 3 || actDepLen == 4)) {
				return false;
			}
			
		} catch(Exception e) {
			return false;
		}
    	return true;
    }
    
}
