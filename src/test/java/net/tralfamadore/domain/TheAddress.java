package net.tralfamadore.domain;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

/**
 * Class: TheAddress
 * Created by billreh on 5/6/17.
 */
@Entity
public class TheAddress {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @Column
    @NotNull
    @Size(min = 5, max = 50)
    private String street;

    @Column
    @NotNull
    @Size(min = 3, max = 50)
    private String city;

    @Column
    @NotNull
    @Size(min = 2, max = 2)
    private String state;

    @NotNull
    @Pattern(regexp = "\\d{5}")
    @Size(min = 5, max = 5)
    private String zipCode;

    @Transient
    private String dontPersistMe;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getZipCode() {
        return zipCode;
    }

    public void setZipCode(String zipCode) {
        this.zipCode = zipCode;
    }
}
