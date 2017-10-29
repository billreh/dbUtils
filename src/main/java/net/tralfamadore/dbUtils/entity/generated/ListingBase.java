package net.tralfamadore.dbUtils.entity.generated;

import javax.persistence.*;
import java.util.List;
import java.util.ArrayList;

@MappedSuperclass
public class ListingBase {
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private Long id;

	@Column(name = "price")
	private Integer price;

	@Column(name = "house_type")
	private String houseType;

	@Column(name = "bathrooms")
	private Double bathrooms;

	@Column(name = "bedrooms")
	private Integer bedrooms;

	@Column(name = "square_feet")
	private Integer squareFeet;

	@Column(name = "main_photo")
	private String mainPhoto;

	@OneToOne(cascade = CascadeType.ALL, orphanRemoval=true, fetch = FetchType.EAGER, targetEntity = net.tralfamadore.dbUtils.entity.Agent.class)
	@JoinColumn(name = "agent_id", referencedColumnName = "id")
	private net.tralfamadore.dbUtils.entity.Agent agent;

	@OneToOne(cascade = CascadeType.ALL, orphanRemoval=true, fetch = FetchType.EAGER, targetEntity = net.tralfamadore.dbUtils.entity.Address.class)
	@JoinColumn(name = "address_id", referencedColumnName = "id")
	private net.tralfamadore.dbUtils.entity.Address address;

	@OneToMany(cascade = CascadeType.ALL, orphanRemoval=true, fetch = FetchType.EAGER, targetEntity = net.tralfamadore.dbUtils.entity.ListingDetail.class)
	@org.hibernate.annotations.Fetch(value = org.hibernate.annotations.FetchMode.SUBSELECT)
	@JoinColumn(name = "listing_id", referencedColumnName = "id", nullable=false)
	private List<net.tralfamadore.dbUtils.entity.ListingDetail> listingDetails =  new ArrayList<>();

	@OneToMany(cascade = CascadeType.ALL, orphanRemoval=true, fetch = FetchType.EAGER, targetEntity = net.tralfamadore.dbUtils.entity.Photo.class)
	@org.hibernate.annotations.Fetch(value = org.hibernate.annotations.FetchMode.SUBSELECT)
	@JoinColumn(name = "listing_id", referencedColumnName = "id", nullable=false)
	private List<net.tralfamadore.dbUtils.entity.Photo> photos =  new ArrayList<>();

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Integer getPrice() {
		return price;
	}

	public void setPrice(Integer price) {
		this.price = price;
	}

	public String getHouseType() {
		return houseType;
	}

	public void setHouseType(String houseType) {
		this.houseType = houseType;
	}

	public Double getBathrooms() {
		return bathrooms;
	}

	public void setBathrooms(Double bathrooms) {
		this.bathrooms = bathrooms;
	}

	public Integer getBedrooms() {
		return bedrooms;
	}

	public void setBedrooms(Integer bedrooms) {
		this.bedrooms = bedrooms;
	}

	public Integer getSquareFeet() {
		return squareFeet;
	}

	public void setSquareFeet(Integer squareFeet) {
		this.squareFeet = squareFeet;
	}

	public String getMainPhoto() {
		return mainPhoto;
	}

	public void setMainPhoto(String mainPhoto) {
		this.mainPhoto = mainPhoto;
	}

	public net.tralfamadore.dbUtils.entity.Agent getAgent() {
		return agent;
	}

	public void setAgent(net.tralfamadore.dbUtils.entity.Agent agent) {
		this.agent = agent;
	}

	public net.tralfamadore.dbUtils.entity.Address getAddress() {
		return address;
	}

	public void setAddress(net.tralfamadore.dbUtils.entity.Address address) {
		this.address = address;
	}

	public List<net.tralfamadore.dbUtils.entity.ListingDetail> getListingDetails() {
		return listingDetails;
	}

	public void setListingDetails(List<net.tralfamadore.dbUtils.entity.ListingDetail> listingDetails) {
		this.listingDetails.clear();
		this.listingDetails.addAll(listingDetails);
	}

	public List<net.tralfamadore.dbUtils.entity.Photo> getPhotos() {
		return photos;
	}

	public void setPhotos(List<net.tralfamadore.dbUtils.entity.Photo> photos) {
		this.photos.clear();
		this.photos.addAll(photos);
	}

}