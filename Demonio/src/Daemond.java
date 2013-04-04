import java.util.concurrent.Delayed;

import com.bg.parser.twitter.twitter;

public class Daemond extends Thread {

	public void run(){
		while(true){
			twitter t = new twitter();
			t.Update_Users();
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public Daemond(){
		this.setDaemon(true);
	}

	public static void main (String args[]) {
		Daemond D = new Daemond();
		D.start();
	}

}
