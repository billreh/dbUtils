package net.tralfamadore.domain;

import javax.persistence.*;
import javax.validation.constraints.Size;
import java.util.List;

/**
 * Class: ParentTable
 * Created by billreh on 5/6/17.
 */
@Entity
public class ParentTable {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @Size(max = 50)
    private String name;

    @OneToOne(cascade = CascadeType.ALL, orphanRemoval=true, fetch = FetchType.EAGER, targetEntity = ChildTable.class)
    @JoinColumn(name = "CHILD_ID", referencedColumnName = "ID")
    private ChildTable childTable;

    @OneToMany(cascade = CascadeType.ALL, orphanRemoval=true, fetch = FetchType.EAGER, targetEntity = ChildrenTable.class)
    @org.hibernate.annotations.Fetch(value = org.hibernate.annotations.FetchMode.SUBSELECT)
    @JoinColumn(name = "PARENT_TABLE_ID", referencedColumnName = "ID", nullable=false)
    private List<ChildrenTable> childrenTables;

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

    public ChildTable getChildTable() {
        return childTable;
    }

    public void setChildTable(ChildTable childTable) {
        this.childTable = childTable;
    }

    public List<ChildrenTable> getChildrenTables() {
        return childrenTables;
    }

    public void setChildrenTables(List<ChildrenTable> childrenTables) {
        this.childrenTables = childrenTables;
    }
}
