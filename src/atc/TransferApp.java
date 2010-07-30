package atc;

import atc.emitter.Emitter;
import org.apache.log4j.BasicConfigurator;
import org.apache.thrift.transport.TTransportException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;

public class TransferApp {

    public static final void main(String[] args) throws TTransportException, IOException {
        //BasicConfigurator.configure();
        ApplicationContext context = new ClassPathXmlApplicationContext(args[0]);
        Emitter emitter = (Emitter)context.getBean(args[1]);
        emitter.begin();
        System.exit(0);
    }
}
