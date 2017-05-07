package net.tralfamadore.domain;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

/**
 * Class: ChildrenTable
 * Created by billreh on 5/7/17.
 */
@Entity
public class ChildrenTable {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @Size(max = 100)
    private String name;

    @NotNull
    private Long parentTableId;

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

    public Long getParentTableId() {
        return parentTableId;
    }

    public void setParentTableId(Long parentTableId) {
        this.parentTableId = parentTableId;
    }
}
