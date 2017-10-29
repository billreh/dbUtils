package net.tralfamadore.dbUtils.entity;

import javax.persistence.*;

import net.tralfamadore.dbUtils.DatabaseUtils;
import net.tralfamadore.dbUtils.entity.generated.AddressBase;

import java.util.List;

@Entity(name = "address")
public class Address extends AddressBase {
    public static List<Address> getAllAddressses() {
        return new DatabaseUtils().sql("select * from address").selectList(Address.class);
    }
}
