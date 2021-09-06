package xquery;

import java.io.*;
import javax.xml.xquery.XQConnection;
import javax.xml.xquery.XQDataSource;
import javax.xml.xquery.XQException;
import javax.xml.xquery.XQPreparedExpression;
import javax.xml.xquery.XQResultSequence;
import com.saxonica.xqj.SaxonXQDataSource;
import xquery.Constants;

public class XqueryCompiler {
    public static void main(String[] args) throws FileNotFoundException, XQException {
        execute();
    }

    private static void execute() throws FileNotFoundException, XQException {
        InputStream inputStream = new FileInputStream(new File(Constants.XQY_SCRIPT));
        XQDataSource ds = new SaxonXQDataSource();
        XQConnection conn = ds.getConnection();
        XQPreparedExpression exp = conn.prepareExpression(inputStream);
        XQResultSequence result = exp.executeQuery();

        while (result.next()) {
            System.out.println(result.getItemAsString(null));
        }
    }
}