package net.tralfamadore.domain;

import net.tralfamadore.Gender;
import net.tralfamadore.IsBinary;

import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.validation.constraints.Size;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Class: TheBean
 * Created by billreh on 5/6/17.
 */
@Entity
public class TheBean {
    @Id
    private Long id;

    @Size(max = 100)
    private String name;

    private LocalDate theDate;

    private LocalDateTime timestamp;

    private double theDouble;

    @Enumerated(EnumType.STRING)
    private Gender gender;

    @Enumerated(EnumType.ORDINAL)
    private IsBinary isBinary;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public LocalDate getTheDate() {
        return theDate;
    }

    public void setTheDate(LocalDate theDate) {
        this.theDate = theDate;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public double getTheDouble() {
        return theDouble;
    }

    public void setTheDouble(double theDouble) {
        this.theDouble = theDouble;
    }
}
