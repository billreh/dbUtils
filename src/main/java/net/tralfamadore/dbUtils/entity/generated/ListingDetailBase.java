package net.tralfamadore.dbUtils.entity.generated;

import javax.persistence.*;
import java.util.List;
import java.util.ArrayList;

@MappedSuperclass
public class ListingDetailBase {
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private Long id;

	@Column(name = "listing_id", insertable = false, updatable = false)
	private Long listingId;

	@Column(name = "overview")
	private String overview;

	@Column(name = "master_bedroom")
	private String masterBedroom;

	@Column(name = "full_bathrooms")
	private Integer fullBathrooms;

	@Column(name = "half_bathrooms")
	private Integer halfBathrooms;

	@Column(name = "dining_kitchen")
	private Integer diningKitchen;

	@Column(name = "dining_room")
	private Integer diningRoom;

	@Column(name = "stories")
	private Integer stories;

	@Column(name = "exterior")
	private String exterior;

	@Column(name = "parking")
	private String parking;

	@Column(name = "status")
	private String status;

	@Column(name = "school_district")
	private String schoolDistrict;

	@Column(name = "style")
	private String style;

	@Column(name = "year_built")
	private Integer yearBuilt;

	@OneToMany(cascade = CascadeType.ALL, orphanRemoval=true, fetch = FetchType.EAGER, targetEntity = net.tralfamadore.dbUtils.entity.OtherRoom.class)
	@org.hibernate.annotations.Fetch(value = org.hibernate.annotations.FetchMode.SUBSELECT)
	@JoinColumn(name = "listing_detail_id", referencedColumnName = "id", nullable=false)
	private List<net.tralfamadore.dbUtils.entity.OtherRoom> otherRooms =  new ArrayList<>();

	@OneToMany(cascade = CascadeType.ALL, orphanRemoval=true, fetch = FetchType.EAGER, targetEntity = net.tralfamadore.dbUtils.entity.ExteriorFeature.class)
	@org.hibernate.annotations.Fetch(value = org.hibernate.annotations.FetchMode.SUBSELECT)
	@JoinColumn(name = "listing_detail_id", referencedColumnName = "id", nullable=false)
	private List<net.tralfamadore.dbUtils.entity.ExteriorFeature> exteriorFeatures =  new ArrayList<>();

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Long getListingId() {
		return listingId;
	}

	public void setListingId(Long listingId) {
		this.listingId = listingId;
	}

	public String getOverview() {
		return overview;
	}

	public void setOverview(String overview) {
		this.overview = overview;
	}

	public String getMasterBedroom() {
		return masterBedroom;
	}

	public void setMasterBedroom(String masterBedroom) {
		this.masterBedroom = masterBedroom;
	}

	public Integer getFullBathrooms() {
		return fullBathrooms;
	}

	public void setFullBathrooms(Integer fullBathrooms) {
		this.fullBathrooms = fullBathrooms;
	}

	public Integer getHalfBathrooms() {
		return halfBathrooms;
	}

	public void setHalfBathrooms(Integer halfBathrooms) {
		this.halfBathrooms = halfBathrooms;
	}

	public Integer getDiningKitchen() {
		return diningKitchen;
	}

	public void setDiningKitchen(Integer diningKitchen) {
		this.diningKitchen = diningKitchen;
	}

	public Integer getDiningRoom() {
		return diningRoom;
	}

	public void setDiningRoom(Integer diningRoom) {
		this.diningRoom = diningRoom;
	}

	public Integer getStories() {
		return stories;
	}

	public void setStories(Integer stories) {
		this.stories = stories;
	}

	public String getExterior() {
		return exterior;
	}

	public void setExterior(String exterior) {
		this.exterior = exterior;
	}

	public String getParking() {
		return parking;
	}

	public void setParking(String parking) {
		this.parking = parking;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getSchoolDistrict() {
		return schoolDistrict;
	}

	public void setSchoolDistrict(String schoolDistrict) {
		this.schoolDistrict = schoolDistrict;
	}

	public String getStyle() {
		return style;
	}

	public void setStyle(String style) {
		this.style = style;
	}

	public Integer getYearBuilt() {
		return yearBuilt;
	}

	public void setYearBuilt(Integer yearBuilt) {
		this.yearBuilt = yearBuilt;
	}

	public List<net.tralfamadore.dbUtils.entity.OtherRoom> getOtherRooms() {
		return otherRooms;
	}

	public void setOtherRooms(List<net.tralfamadore.dbUtils.entity.OtherRoom> otherRooms) {
		this.otherRooms.clear();
		this.otherRooms.addAll(otherRooms);
	}

	public List<net.tralfamadore.dbUtils.entity.ExteriorFeature> getExteriorFeatures() {
		return exteriorFeatures;
	}

	public void setExteriorFeatures(List<net.tralfamadore.dbUtils.entity.ExteriorFeature> exteriorFeatures) {
		this.exteriorFeatures.clear();
		this.exteriorFeatures.addAll(exteriorFeatures);
	}

}