package hdfs;

import java.io.Serializable;

public class Node implements Serializable {

	/**
	 * class contient les donnees pour que le client communique avec un dataNode
	 */
	private static final long serialVersionUID = 1L;

	protected String nodeIP;
    protected int hidoopPort;
    protected int hdfsPort;
	protected String pathData;

	public Node(String nodeIP, int hidoopPort, int hdfsPort, String pathData) {
		super();
		this.nodeIP = nodeIP;
		this.hidoopPort = hidoopPort;
		this.hdfsPort = hdfsPort;
        this.pathData = pathData;
		//this.id = id;
	}


	public String getNodeIP() {
		return nodeIP;
	}
	public int getHidoopPort() {
		return hidoopPort;
	}
	public int getHdfsPort() {
		return hdfsPort;
	}
	public String getPathData() {
        return pathData;
    }
    public String getRMIUrl(boolean hidoop){
    	return nodeIP +":" + (hidoop ? hidoopPort : hdfsPort);
	}

    @Override
	public String toString() {
		return "NodeIdentifier [nodeIP=" + nodeIP + ", hidoopPort=" + hidoopPort + "]";
	}


}