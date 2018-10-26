import java.io.File;
import java.util.Calendar;
import java.util.Date;

public class Test {

    public static void main(String args[]){
//        new File(".").list((dir, name) -> true
//        );
        long v = 1540203919896L;
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(v);
        Date date = cal.getTime();
        System.out.println(date);

        cal.setTimeInMillis(1540203913288L);
        date = cal.getTime();
        System.out.println(date);

    }
}
