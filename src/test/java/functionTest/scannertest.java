package functionTest;

import javax.lang.model.util.Types;
import java.io.File;
import java.io.FileNotFoundException;

import java.sql.Timestamp;
import java.util.Arrays;
import java.sql.Date;
import java.util.Calendar;
import java.util.Scanner;

public class scannertest {
    public static void main(String[] args) throws FileNotFoundException {

        Date date1 = Date.valueOf("1993-09-17");
        Calendar cal = Calendar.getInstance();
        cal.setTime(date1);
        cal.add(Calendar.YEAR,1);
        Date date2 = new Date(cal.getTimeInMillis());

        System.out.println(date1 + " " + date2);

    }
}
