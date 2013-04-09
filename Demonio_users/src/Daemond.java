import java.util.concurrent.Delayed;

import com.bg.parser.twitter.twitter;

/**
 *  this class is used for the daemond (Thread)
 * @author KRISTIAN
 *
 */
//public class Daemond extends Thread{
public class Daemond{
	
	/**
	 * the run function is used to execute the procedure
	 */
	public void run(){
		while(true){
			twitter t = new twitter();
			t.Update_Users();
			try {
				System.out.println("Waiting...");
				Thread.sleep(1000000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 *  To set the deamond as Daemond
	 */
	public Daemond(){
		//this.setDaemon(true);
	}

	public static void main (String args[]) {
		Daemond D = new Daemond();
		//D.start();
		D.run();
	}

}
