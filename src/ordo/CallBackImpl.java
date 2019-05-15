package ordo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Semaphore;

public class CallBackImpl extends UnicastRemoteObject implements CallBack  {

	private Semaphore call;

	public CallBackImpl() throws RemoteException {
		this.call = new Semaphore(1);
		try {
			this.call.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onFinished() throws RemoteException {
		this.call.release();
	}

	@Override
	public Semaphore getCall() {	// Retourne la Sémaphore
		return this.call;
	}


}
