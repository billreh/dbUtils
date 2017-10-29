package net.tralfamadore.dbUtils.entity.generated;

import javax.persistence.*;
import java.time.LocalDate;
import java.time.LocalDateTime;

@MappedSuperclass
public class TestmeBase {
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private Long id;

	@Column(name = "stringVal")
	private String stringval;

	@Column(name = "doubleVal")
	private Double doubleval;

	@Column(name = "dateVal")
	private LocalDate dateval;

	@Column(name = "timestameVal")
	private LocalDateTime timestameval;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getStringval() {
		return stringval;
	}

	public void setStringval(String stringval) {
		this.stringval = stringval;
	}

	public Double getDoubleval() {
		return doubleval;
	}

	public void setDoubleval(Double doubleval) {
		this.doubleval = doubleval;
	}

	public LocalDate getDateval() {
		return dateval;
	}

	public void setDateval(LocalDate dateval) {
		this.dateval = dateval;
	}

	public LocalDateTime getTimestameval() {
		return timestameval;
	}

	public void setTimestameval(LocalDateTime timestameval) {
		this.timestameval = timestameval;
	}

}