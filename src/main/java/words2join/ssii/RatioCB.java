package words2join.ssii;

import org.apache.spark.sql.Row;


public class RatioCB {
	private Integer idItem;
	private Integer idTentativo;
	private Integer indiceSimilitud;
	

	public RatioCB(Row _x, Integer index) {
		idItem = _x.getInt(0);
		idTentativo = _x.getInt(1);
		indiceSimilitud = index;
	}

	public Integer getIdTentativo() {
		return idTentativo;
	}

	public void setIdTentativo(Integer idTentativo) {
		this.idTentativo = idTentativo;
	}

	public Integer getIndiceSimilitud() {
		return indiceSimilitud;
	}

	public void setIndiceSimilitud(Integer indiceSimilitud) {
		this.indiceSimilitud = indiceSimilitud;
	}

	public Integer getIdItem() {
		return idItem;
	}
	public void setIdItem(Integer idItem) {
		this.idItem = idItem;
	}

	@Override
	public String toString() {
		return "RatioCB [idItem=" + idItem + ", idTentativo=" + idTentativo + ", indiceSimilitud=" + indiceSimilitud
				+ "]";
	}

}
