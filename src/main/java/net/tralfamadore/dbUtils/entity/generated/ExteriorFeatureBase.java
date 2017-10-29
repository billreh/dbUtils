package net.tralfamadore.dbUtils.entity.generated;

import javax.persistence.*;

@MappedSuperclass
public class ExteriorFeatureBase {
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private Long id;

	@Column(name = "listing_detail_id", insertable = false, updatable = false)
	private Long listingDetailId;

	@Column(name = "name")
	private String name;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Long getListingDetailId() {
		return listingDetailId;
	}

	public void setListingDetailId(Long listingDetailId) {
		this.listingDetailId = listingDetailId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}