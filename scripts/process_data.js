import fs from 'fs';
import config from '../config.js';
import * as turf from '@turf/turf';
import { createParseStream } from 'big-json';

const OUTPUT_DECIMAL_PLACES = Number.isFinite(config.outputPrecision?.decimals)
  ? config.outputPrecision.decimals
  : null;

const ROAD_FILTERS = {
  excludeHighways: config.roadFilters?.excludeHighways
    ? new Set(config.roadFilters.excludeHighways)
    : null,
  minLengthMeters: Number.isFinite(config.roadFilters?.minLengthMeters)
    ? config.roadFilters.minLengthMeters
    : null,
};
// configure your own population groups at demand points, base game targets ~20 pops
const POPULATION_GROUP_CONFIG = {
  targetSize: Number.isFinite(config.populationChunking?.targetSize)
    ? config.populationChunking.targetSize
    : 160,
  minSize: Number.isFinite(config.populationChunking?.minSize)
    ? config.populationChunking.minSize
    : 20,
  minimumFinalizeSize: Number.isFinite(config.populationChunking?.minimumFinalizeSize)
    ? config.populationChunking.minimumFinalizeSize
    : 30,
  maxSize: Number.isFinite(config.populationChunking?.maxSize)
    ? config.populationChunking.maxSize
    : 380,
  maxConnectionsPerPoint: Number.isFinite(config.populationChunking?.maxConnectionsPerPoint)
    ? config.populationChunking.maxConnectionsPerPoint
    : 24,
};

const roundNumber = (value) => {
  if (OUTPUT_DECIMAL_PLACES === null) return value;
  if (typeof value !== 'number') return value;
  if (!Number.isFinite(value)) return value;
  if (Number.isInteger(value)) return value;
  return Number(value.toFixed(OUTPUT_DECIMAL_PLACES));
};

const writeJson = (filePath, data) => {
  const replacer = OUTPUT_DECIMAL_PLACES === null
    ? null
    : (_, value) => roundNumber(value);
  fs.writeFileSync(filePath, JSON.stringify(data, replacer), { encoding: 'utf8' });
};

const hashString = (input) => {
  let hash = 0;
  for (let i = 0; i < input.length; i++) {
    hash = (hash * 31 + input.charCodeAt(i)) | 0;
  }
  return Math.abs(hash);
};

const splitConnectionIntoGroups = (size, residenceId, jobId) => {
  if (!size || size <= 0) return [];
  if (!POPULATION_GROUP_CONFIG.maxSize || size <= POPULATION_GROUP_CONFIG.minSize) return [size];

  const minGroupCount = Math.max(1, Math.ceil(size / POPULATION_GROUP_CONFIG.maxSize));
  const maxGroupCount = Math.max(
    minGroupCount,
    POPULATION_GROUP_CONFIG.minSize > 0
      ? Math.floor(size / POPULATION_GROUP_CONFIG.minSize)
      : size
  );

  let groupCount = Math.max(minGroupCount, Math.round(size / POPULATION_GROUP_CONFIG.targetSize));
  if (groupCount > maxGroupCount) groupCount = maxGroupCount;
  if (groupCount < 1) groupCount = 1;

  let baseSize = Math.floor(size / groupCount);
  while (groupCount > 1 && baseSize < POPULATION_GROUP_CONFIG.minSize) {
    groupCount -= 1;
    baseSize = Math.floor(size / groupCount);
  }

  let sizes = new Array(groupCount).fill(baseSize);
  let remainder = size - baseSize * groupCount;
  if (remainder < 0) remainder = 0;

  if (sizes.length) {
    const hash = hashString(`${residenceId}-${jobId}`);
    for (let i = 0; remainder > 0; i++, remainder--) {
      const index = (hash + i) % sizes.length;
      sizes[index] += 1;
    }
  }

  if (POPULATION_GROUP_CONFIG.maxSize) {
    const adjusted = [];
    sizes.forEach((value) => {
      if (value <= POPULATION_GROUP_CONFIG.maxSize) {
        adjusted.push(value);
        return;
      }
      let remainingValue = value;
      while (remainingValue > POPULATION_GROUP_CONFIG.maxSize) {
        adjusted.push(POPULATION_GROUP_CONFIG.maxSize);
        remainingValue -= POPULATION_GROUP_CONFIG.maxSize;
      }
      if (remainingValue > 0) adjusted.push(remainingValue);
    });
    sizes = adjusted;
  }

  if (POPULATION_GROUP_CONFIG.minSize && sizes.length > 1) {
    for (let i = 0; i < sizes.length; i++) {
      if (sizes[i] >= POPULATION_GROUP_CONFIG.minSize) continue;
      const neighborIndex = i === 0 ? 1 : i - 1;
      if (neighborIndex < sizes.length) {
        sizes[neighborIndex] += sizes[i];
        sizes.splice(i, 1);
        i -= 1;
      }
    }
  }

  if (POPULATION_GROUP_CONFIG.minimumFinalizeSize && sizes.length > 1) {
    const sorted = [...sizes].sort((a, b) => a - b);
    const merged = [];
    let accumulator = 0;

    sorted.forEach((value) => {
      if (value >= POPULATION_GROUP_CONFIG.minimumFinalizeSize) {
        if (accumulator > 0) {
          merged.push(accumulator);
          accumulator = 0;
        }
        merged.push(value);
      } else {
        accumulator += value;
        if (accumulator >= POPULATION_GROUP_CONFIG.minimumFinalizeSize) {
          merged.push(accumulator);
          accumulator = 0;
        }
      }
    });

    if (accumulator > 0) {
      if (merged.length) {
        merged[merged.length - 1] += accumulator;
      } else {
        merged.push(accumulator);
      }
    }

    sizes = merged;
  }

  if (POPULATION_GROUP_CONFIG.maxSize) {
    const adjusted = [];
    sizes.forEach((value) => {
      if (value <= POPULATION_GROUP_CONFIG.maxSize) {
        adjusted.push(value);
        return;
      }
      let remainingValue = value;
      while (remainingValue > POPULATION_GROUP_CONFIG.maxSize) {
        adjusted.push(POPULATION_GROUP_CONFIG.maxSize);
        remainingValue -= POPULATION_GROUP_CONFIG.maxSize;
      }
      if (remainingValue > 0) adjusted.push(remainingValue);
    });
    sizes = adjusted;
  }

  return sizes;
};

const optimizeBuilding = (unOptimizedBuilding) => {
  return {
    b: [unOptimizedBuilding.minX, unOptimizedBuilding.minY, unOptimizedBuilding.maxX, unOptimizedBuilding.maxY],
    f: unOptimizedBuilding.foundationDepth,
    p: unOptimizedBuilding.polygon,
  }
};

const optimizeIndex = (unOptimizedIndex) => {
  return {
    cs: unOptimizedIndex.cellHeightCoords,
    bbox: [unOptimizedIndex.minLon, unOptimizedIndex.minLat, unOptimizedIndex.maxLon, unOptimizedIndex.maxLat],
    grid: [unOptimizedIndex.cols, unOptimizedIndex.rows],
    cells: Object.keys(unOptimizedIndex.cells).map((key) => [...key.split(',').map((n) => Number(n)), ...unOptimizedIndex.cells[key]]),
    buildings: unOptimizedIndex.buildings.map((unOptimizedBuilding) => optimizeBuilding(unOptimizedBuilding)),
    stats: {
      count: unOptimizedIndex.buildings.length,
      maxDepth: unOptimizedIndex.maxDepth,
    }
  }
};

// how much square footage we should probably expect per resident of this housing type
// later on ill calculate the cross section of the building's square footage, 
// then multiply that but the total number of floors to get an approximate full square footage number
// i can then divide by the below number to get a rough populaion stat
const squareFeetPerPopulation = {
  yes: 600, // most likely a SFH
  apartments: 240,
  barracks: 100, // google said 70-90, but imma bump it up a bit tbh
  bungalow: 600, // sfh
  cabin: 600, // sfh
  detached: 600, // sfh
  annexe: 240, // kinda like apartments
  dormitory: 125, // good lord
  farm: 600, // sfh
  ger: 240, // technically sfh, but generally usually smaller and more compact. honorary apartment. TIL "ger" is mongolian for the english word "yurt"
  hotel: 240, // gonna count these as apartments because hotel guests use transit too
  house: 600, // sfh
  houseboat: 600, // interdasting
  residential: 600, // could be anything, but im assuimg sfh here
  semidetached_house: 400, // duplex
  static_caravan: 500,
  stilt_house: 600,
  terrace: 500, // townhome
  tree_house: 240, // fuck it
  trullo: 240, // there is nothing scientific here, its all fucking vibes
};

const squareFeetPerJob = {
  commercial: 150, // non specific, restaraunts i guess?
  industrial: 500, // vibes vibes vibes vibes!!!!!,
  kiosk: 50, // its all vibes baby
  office: 150, // all of my vibes are 100% meat created
  retail: 300,
  supermarket: 300,
  warehouse: 500,
  // the following are all religious and im assuming ~100 square feet, not for job purposes, 
  // but for the fact that people go to religious institutions
  // might use a similar trick for sports stadiums
  religious: 100,
  cathedral: 100,
  chapel: 100,
  church: 100,
  kingdom_hall: 100,
  monastery: 100,
  mosque: 100,
  presbytery: 100,
  shrine: 100,
  synagogue: 100,
  temple: 100,
  // end of religious
  bakehouse: 300,
  college: 250, // collge/uni is a job
  fire_station: 500,
  government: 150,
  gatehouse: 150,
  hospital: 150,
  kindergarten: 100,
  museum: 300,
  public: 300,
  school: 100,
  train_station: 1000,
  transportation: 1000,
  university: 250,
  // sports time! im going to treat these like offices because i said so.
  // i think itll end up creating demand thats on average what stadiums see traffic wise. not sure
  grandstand: 150,
  pavilion: 150,
  riding_hall: 150,
  sports_hall: 150,
  sports_centre: 150,
  stadium: 150,
};

const processPlaceConnections = (place, rawBuildings, rawPlaces) => {
  let neighborhoods = {};
  let centersOfNeighborhoods = {};
  let calculatedBuildings = {};

  const basePlaceTypes = new Set(['quarter', 'neighbourhood']);
  const sprawlPlaceTypes = new Set(['suburb', 'town', 'village', 'hamlet', 'isolated_dwelling', 'locality', 'residential', 'city']);
  const ruralCandidates = [];
  const clusterDistanceByPlaceType = {
    isolated_dwelling: 0.35,
    hamlet: 0.45,
    village: 0.6,
    town: 0.8,
    suburb: 0.75,
    residential: 0.7,
    locality: 0.5,
    neighbourhood: 0.45,
    quarter: 0.5,
    city: 0.9,
  };
  const ruralRadiusByPlaceType = {
    isolated_dwelling: 1,
    hamlet: 1.25,
    village: 2,
    town: 3,
    suburb: 2.5,
    residential: 2.25,
    locality: 1.5,
    neighbourhood: 1.75,
    quarter: 2,
    city: 4,
  };
  const orphanCellSizeDegrees = 0.01;
  const orphanPopulationThreshold = 25;
  const orphanAdoptionThreshold = 0.9; // km
  const orphanInitialClusterDistance = 0.6;
  const maxClusterPopulation = 7500;
  const minClusterSplitDistance = 0.15;

  const computePlaceCenter = (placeFeature) => {
    if (placeFeature.type == 'node') return [placeFeature.lon, placeFeature.lat];
    if ((placeFeature.type == 'way' || placeFeature.type == 'relation') && placeFeature.bounds) {
      return [
        (placeFeature.bounds.minlon + placeFeature.bounds.maxlon) / 2,
        (placeFeature.bounds.minlat + placeFeature.bounds.maxlat) / 2,
      ];
    }
    return null;
  };

  const directPlaces = [];

  // finding areas of neighborhoods and separating candidates that need splitting
  rawPlaces.forEach((placeFeature) => {
    if (!placeFeature.tags?.place) return;
    const placeType = placeFeature.tags.place;

    if (basePlaceTypes.has(placeType)) {
      directPlaces.push(placeFeature);
      return;
    }

    if (sprawlPlaceTypes.has(placeType)) {
      ruralCandidates.push(placeFeature);
    }
  });

  // sorting buildings between residential and commercial
  rawBuildings.forEach((building) => {
    if (building.tags.building) { // should always be true, but why not
      const __coords = building.geometry.map((point) => [point.lon, point.lat]);
      if (__coords.length < 3) return;
      if (__coords[0][0] !== __coords[__coords.length - 1][0] || __coords[0][1] !== __coords[__coords.length - 1][1]) __coords.push(__coords[0]);
      const buildingGeometry = turf.polygon([__coords]);
      let buildingAreaMultiplier = Math.max(Number(building.tags['building:levels']), 1); // assuming a single story if no level data
      if (isNaN(buildingAreaMultiplier)) buildingAreaMultiplier = 1;
      const buildingArea = turf.area(buildingGeometry) * buildingAreaMultiplier * 10.7639; // that magic number converts from square meters to square feet
      const buildingCenter = [(building.bounds.minlon + building.bounds.maxlon) / 2, (building.bounds.minlat + building.bounds.maxlat) / 2];

      if (squareFeetPerPopulation[building.tags.building]) { // residential
        const approxPop = Math.floor(buildingArea / squareFeetPerPopulation[building.tags.building]);
        calculatedBuildings[building.id] = {
          ...building,
          approxPop,
          buildingCenter,
        };
      } else if (squareFeetPerJob[building.tags.building]) { // commercial/jobs
        const approxJobs = Math.floor(buildingArea / squareFeetPerJob[building.tags.building]);

        calculatedBuildings[building.id] = {
          ...building,
          approxJobs,
          buildingCenter,
        };
      }
    }
  });

  const residentialBuildingPoints = turf.featureCollection(
    Object.values(calculatedBuildings)
      .filter((building) => (building.approxPop ?? 0) > 0)
      .map((building) =>
        turf.point(building.buildingCenter, {
          buildingID: building.id,
          approxPop: building.approxPop,
        })
      )
  );

  const getBuildingsForPlace = (placeFeature) => {
    if (!residentialBuildingPoints.features.length) return [];

    const placeType = placeFeature.tags?.place;

    if ((placeFeature.type == 'way' || placeFeature.type == 'relation') && placeFeature.geometry?.length >= 3) {
      const ring = placeFeature.geometry.map((coord) => [coord.lon, coord.lat]);
      if (ring[0][0] !== ring[ring.length - 1][0] || ring[0][1] !== ring[ring.length - 1][1]) ring.push(ring[0]);
      const polygon = turf.polygon([ring]);
      return residentialBuildingPoints.features.filter((feature) => turf.booleanPointInPolygon(feature, polygon));
    }

    if (placeFeature.bounds) {
      return residentialBuildingPoints.features.filter((feature) => {
        const [lon, lat] = feature.geometry.coordinates;
        return (
          lon >= placeFeature.bounds.minlon &&
          lon <= placeFeature.bounds.maxlon &&
          lat >= placeFeature.bounds.minlat &&
          lat <= placeFeature.bounds.maxlat
        );
      });
    }

    const center = computePlaceCenter(placeFeature);
    if (!center) return [];

    const radius = ruralRadiusByPlaceType[placeType] ?? 2;
    const centerPoint = turf.point(center);
    return residentialBuildingPoints.features.filter((feature) => turf.distance(centerPoint, feature, { units: 'kilometers' }) <= radius);
  };

  let syntheticClusterCounter = 0;

  const addCenterFromFeature = (placeFeature, centerCoords, nameSuffix) => {
    const baseId = placeFeature.id?.toString?.() ?? `place-${syntheticClusterCounter}`;
    const syntheticId = nameSuffix ? `${baseId}-${nameSuffix}` : baseId;

    neighborhoods[syntheticId] = {
      ...placeFeature,
      id: syntheticId,
      tags: {
        ...placeFeature.tags,
        ...(nameSuffix
          ? {
              name: placeFeature.tags?.name
                ? `${placeFeature.tags.name} ${nameSuffix}`
                : nameSuffix,
              'subwaybuilder:synthetic': 'true',
            }
          : {}),
      },
    };
    centersOfNeighborhoods[syntheticId] = centerCoords;
    syntheticClusterCounter += 1;
  };

  const deriveNameFromBuildings = (buildingFeatures) => {
    const nameScores = {};
    buildingFeatures.forEach((feature) => {
      const building = calculatedBuildings[feature.properties.buildingID];
      if (!building?.tags) return;
      const tags = building.tags;
      const candidates = [
        tags['addr:city'],
        tags['addr:town'],
        tags['addr:village'],
        tags['addr:hamlet'],
        tags['addr:suburb'],
        tags['addr:place'],
      ].filter(Boolean);
      if (!candidates.length && tags.name) candidates.push(tags.name);
      const weight = building.approxPop ?? 1;
      candidates.forEach((candidate) => {
        nameScores[candidate] = (nameScores[candidate] || 0) + weight;
      });
    });

    const sorted = Object.entries(nameScores).sort((a, b) => b[1] - a[1]);
    return sorted.length ? sorted[0][0] : null;
  };

  const organizeDbscanGroups = (features) => {
    const groups = {};
    features.forEach((feature) => {
      const clusterId = feature.properties.cluster;
      const key = clusterId !== undefined && clusterId !== null && clusterId !== -1
        ? clusterId
        : `noise-${feature.properties.buildingID}`;
      if (!groups[key]) groups[key] = [];
      groups[key].push(feature);
    });
    return Object.values(groups);
  };

  const splitLargeClusterIfNeeded = (groupFeatures, baseDistance, depth = 0) => {
    const totalPop = groupFeatures.reduce((sum, feature) => sum + (feature.properties?.approxPop ?? 0), 0);
    if (totalPop <= maxClusterPopulation || groupFeatures.length < 3 || baseDistance <= minClusterSplitDistance) {
      if (totalPop > maxClusterPopulation && groupFeatures.length >= 2 && baseDistance <= minClusterSplitDistance) {
        const lons = groupFeatures.map((feature) => feature.geometry.coordinates[0]);
        const lats = groupFeatures.map((feature) => feature.geometry.coordinates[1]);
        const lonRange = Math.max(...lons) - Math.min(...lons);
        const latRange = Math.max(...lats) - Math.min(...lats);
        const sortAxis = lonRange >= latRange ? 0 : 1;
        const sorted = [...groupFeatures].sort((a, b) => a.geometry.coordinates[sortAxis] - b.geometry.coordinates[sortAxis]);
        const mid = Math.ceil(sorted.length / 2);
        const left = sorted.slice(0, mid);
        const right = sorted.slice(mid);
        if (left.length && right.length) {
          return [
            ...splitLargeClusterIfNeeded(left, baseDistance, depth + 1),
            ...splitLargeClusterIfNeeded(right, baseDistance, depth + 1),
          ];
        }
      }
      return [groupFeatures];
    }

    const nextDistance = Math.max(baseDistance * 0.7, minClusterSplitDistance);
    const subClustered = turf.clustersDbscan(
      turf.featureCollection(groupFeatures),
      nextDistance,
      { minPoints: 1, units: 'kilometers' }
    );
    const subGroups = organizeDbscanGroups(subClustered.features);

    if (subGroups.length === 1 && subGroups[0].length === groupFeatures.length) {
      return [groupFeatures];
    }

    return subGroups.flatMap((subGroup) => splitLargeClusterIfNeeded(subGroup, nextDistance, depth + 1));
  };

  const clusterPlaceFeature = (placeFeature) => {
    const placeType = placeFeature.tags?.place;
    const placeDistance = clusterDistanceByPlaceType[placeType] ?? 0.6;
    const placeBuildings = getBuildingsForPlace(placeFeature);

    if (!placeBuildings.length) {
      const center = computePlaceCenter(placeFeature);
      if (center) addCenterFromFeature(placeFeature, center);
      return;
    }

    const totalPlacePopulation = placeBuildings.reduce((sum, feature) => sum + (feature.properties?.approxPop ?? 0), 0);
    if (totalPlacePopulation === 0) {
      const center = computePlaceCenter(placeFeature);
      if (center) addCenterFromFeature(placeFeature, center);
      return;
    }

    const clustered = turf.clustersDbscan(
      turf.featureCollection(placeBuildings),
      placeDistance,
      { minPoints: 1, units: 'kilometers' }
    );

    const groups = organizeDbscanGroups(clustered.features)
      .filter((group) => group.length);

    const finalGroups = groups.flatMap((group) => splitLargeClusterIfNeeded(group, placeDistance))
      .filter((group) => group.length && group.reduce((sum, feature) => sum + (feature.properties?.approxPop ?? 0), 0) > 0);

    if (!finalGroups.length) {
      const center = computePlaceCenter(placeFeature);
      if (center) addCenterFromFeature(placeFeature, center);
      return;
    }

    finalGroups.forEach((finalGroup, index) => {
      const centerPoint = turf.centerOfMass(turf.featureCollection(finalGroup));
      const useSuffix = finalGroups.length > 1;
      const clusterLabel = useSuffix ? `Cluster ${index + 1}` : null;

      addCenterFromFeature(
        placeFeature,
        centerPoint.geometry.coordinates,
        clusterLabel
      );
    });
  };

  directPlaces.forEach((placeFeature) => clusterPlaceFeature(placeFeature));
  ruralCandidates.forEach((placeFeature) => clusterPlaceFeature(placeFeature));

  const centerPoints = Object.entries(centersOfNeighborhoods).map(([placeID, coords]) =>
    turf.point(coords, { placeID })
  );

  const addSyntheticOrphanClusters = () => {
    if (!residentialBuildingPoints.features.length) return;

    const orphanCells = {};

    residentialBuildingPoints.features.forEach((feature) => {
      const [lon, lat] = feature.geometry.coordinates;
      const approxPop = feature.properties?.approxPop ?? 0;
      const lonKey = Math.floor(lon / orphanCellSizeDegrees);
      const latKey = Math.floor(lat / orphanCellSizeDegrees);
      const cellKey = `${lonKey}:${latKey}`;
      if (!orphanCells[cellKey]) {
        orphanCells[cellKey] = {
          features: [],
          population: 0,
          lonSum: 0,
          latSum: 0,
        };
      }
      orphanCells[cellKey].features.push(feature);
      orphanCells[cellKey].population += approxPop;
      const weight = approxPop || 1;
      orphanCells[cellKey].lonSum += lon * weight;
      orphanCells[cellKey].latSum += lat * weight;
    });

    const candidateCells = Object.values(orphanCells)
      .map((cell) => {
        const totalWeight = cell.population || cell.features.length;
        return {
          ...cell,
          center: [
            cell.lonSum / totalWeight,
            cell.latSum / totalWeight,
          ],
        };
      })
      .filter((cell) => cell.population >= orphanPopulationThreshold);

    candidateCells.sort((a, b) => b.population - a.population);

    candidateCells.forEach((cell) => {
      const centerPoint = turf.point(cell.center);
      let minDistance = Infinity;
      centerPoints.forEach((centerFeature) => {
        const distance = turf.distance(centerPoint, centerFeature, { units: 'kilometers' });
        if (distance < minDistance) minDistance = distance;
      });

      if (centerPoints.length && minDistance <= orphanAdoptionThreshold) return;

      const splitGroups = splitLargeClusterIfNeeded(cell.features, orphanInitialClusterDistance);

      splitGroups.forEach((group) => {
        if (!group.length) return;
        const groupPopulation = group.reduce((sum, feature) => sum + (feature.properties?.approxPop ?? 0), 0);
        if (groupPopulation === 0) return;

        const clusterCenter = turf.centerOfMass(turf.featureCollection(group));
        const derivedName = deriveNameFromBuildings(group);
        const syntheticId = `synthetic-${syntheticClusterCounter}`;
        neighborhoods[syntheticId] = {
          type: 'synthetic',
          id: syntheticId,
          tags: {
            name: derivedName ?? `Synthetic Cluster ${syntheticClusterCounter}`,
            'subwaybuilder:synthetic': 'true',
            'subwaybuilder:cluster-source': 'orphan-residential',
          },
        };
        centersOfNeighborhoods[syntheticId] = clusterCenter.geometry.coordinates;
        centerPoints.push(turf.point(clusterCenter.geometry.coordinates, { placeID: syntheticId }));
        syntheticClusterCounter += 1;
      });
    });
  };

  addSyntheticOrphanClusters();

  if (Object.keys(centersOfNeighborhoods).length === 0) {
    const fallbackBuilding = Object.values(calculatedBuildings)[0];
    if (!fallbackBuilding) return { points: [], pops: [] };

    neighborhoods['generated-cluster'] = {
      id: 'generated-cluster',
      tags: {
        name: 'Generated Cluster',
        'subwaybuilder:synthetic': 'true',
      },
    };
    centersOfNeighborhoods['generated-cluster'] = fallbackBuilding.buildingCenter;
  }

  const centersOfNeighborhoodsFeatureCollection = turf.featureCollection(
    Object.keys(centersOfNeighborhoods).map((placeID) =>
      turf.point(centersOfNeighborhoods[placeID], {
        placeID,
        name: neighborhoods[placeID]?.tags?.name,
      })
    )
  );

  // splitting everything into areas
  const voronoi = turf.voronoi(centersOfNeighborhoodsFeatureCollection, {
    bbox: place.bbox,
  })
  voronoi.features = voronoi.features.filter((feature) => feature);

  // so we can do like, stuff with it
  const buildingsAsFeatureCollection = turf.featureCollection(
    Object.values(calculatedBuildings).map((building) =>
      turf.point(building.buildingCenter, { buildingID: building.id })
    )
  );

  let totalPopulation = 0;
  let totalJobs = 0;
  let finalVoronoiMembers = {}; // what buildings are in each voronoi
  let finalVoronoiMetadata = {}; // additional info on population and jobs

  voronoi.features.forEach((feature) => {
    const buildingsWhichExistWithinFeature = turf.pointsWithinPolygon(buildingsAsFeatureCollection, feature);
    finalVoronoiMembers[feature.properties.placeID] = buildingsWhichExistWithinFeature.features;
    const finalFeature = {
      ...feature.properties,
      totalPopulation: 0,
      totalJobs: 0,
      percentOfTotalPopulation: null,
      percentOfTotalJobs: null,
    };

    buildingsWhichExistWithinFeature.features.forEach((feature) => {
      const building = calculatedBuildings[feature.properties.buildingID];
      finalFeature.totalPopulation += (building.approxPop ?? 0);
      finalFeature.totalJobs += (building.approxJobs ?? 0);
      totalPopulation += (building.approxPop ?? 0);
      totalJobs += (building.approxJobs ?? 0);
    });

    finalVoronoiMetadata[feature.properties.placeID] = finalFeature;
  });

  let finalNeighborhoods = {};
  let neighborhoodConnections = [];

  // creating total percents and setting up final dicts
  Object.values(finalVoronoiMetadata).forEach((place) => {
    finalVoronoiMetadata[place.placeID].percentOfTotalPopulation = place.totalPopulation / totalPopulation;
    finalVoronoiMetadata[place.placeID].percentOfTotalJobs = place.totalJobs / totalJobs;

    finalNeighborhoods[place.placeID] = {
      id: place.placeID,
      location: centersOfNeighborhoods[place.placeID],
      jobs: place.totalJobs,
      residents: place.totalPopulation,
      popIds: [],
    }
  });

  const placeEntries = Object.values(finalVoronoiMetadata);
  let connectionCounter = 0;

  placeEntries.forEach((outerPlace) => {
    const totalPop = Math.round(outerPlace.totalPopulation);
    if (!totalPop || totalPop <= 0) return;

    const connectionCandidates = placeEntries
      .filter((innerPlace) => innerPlace.totalJobs > 0)
      .map((innerPlace) => {
        const connectionLine = turf.lineString([
          centersOfNeighborhoods[outerPlace.placeID],
          centersOfNeighborhoods[innerPlace.placeID],
        ]);
        const connectionDistance = turf.length(connectionLine, { units: 'meters' });
        return {
          residenceId: outerPlace.placeID,
          jobId: innerPlace.placeID,
          weight: innerPlace.percentOfTotalJobs,
          drivingDistance: Math.round(connectionDistance),
          drivingSeconds: Math.round(connectionDistance * 0.12),
        };
      });

    if (!connectionCandidates.length) return;

    connectionCandidates.sort((a, b) => b.weight - a.weight);
    const limitedCandidates = POPULATION_GROUP_CONFIG.maxConnectionsPerPoint
      ? connectionCandidates.slice(0, POPULATION_GROUP_CONFIG.maxConnectionsPerPoint)
      : connectionCandidates;

    let weightSum = limitedCandidates.reduce((sum, candidate) => sum + candidate.weight, 0);
    if (!weightSum || weightSum <= 0) weightSum = limitedCandidates.length;

    const rawConnections = limitedCandidates.map((candidate) => {
      const rawSize = (candidate.weight / weightSum) * totalPop;
      return {
        ...candidate,
        baseSize: Math.floor(rawSize),
        remainder: rawSize - Math.floor(rawSize),
      };
    });

    let assigned = rawConnections.reduce((sum, conn) => sum + conn.baseSize, 0);
    let remaining = totalPop - assigned;
    if (remaining < 0) remaining = 0;

    rawConnections.sort((a, b) => b.remainder - a.remainder);

    for (let i = 0; remaining > 0 && rawConnections.length > 0; i++, remaining--) {
      rawConnections[i % rawConnections.length].baseSize += 1;
    }

    if (POPULATION_GROUP_CONFIG.minimumFinalizeSize) {
      const eligibleConnections = rawConnections.filter((conn) => conn.baseSize > 0);
      if (eligibleConnections.length > 1) {
        let carry = 0;
        eligibleConnections.forEach((conn) => {
          if (conn.baseSize < POPULATION_GROUP_CONFIG.minimumFinalizeSize) {
            carry += conn.baseSize;
            conn.baseSize = 0;
          }
        });

        const donors = eligibleConnections
          .filter((conn) => conn.baseSize >= POPULATION_GROUP_CONFIG.minimumFinalizeSize)
          .sort((a, b) => b.baseSize - a.baseSize);

        if (!donors.length && carry > 0) {
          const largest = eligibleConnections.sort((a, b) => b.baseSize - a.baseSize)[0];
          if (largest) {
            largest.baseSize += carry;
            carry = 0;
          }
        }

        let donorIndex = 0;
        while (carry > 0 && donors.length) {
          donors[donorIndex].baseSize += 1;
          carry -= 1;
          donorIndex = (donorIndex + 1) % donors.length;
        }

        if (carry > 0) {
          const fallback = eligibleConnections.find((conn) => conn.baseSize === 0);
          if (fallback) fallback.baseSize += carry;
        }
      }
    }

    rawConnections.forEach((conn) => {
      if (conn.baseSize <= 0) return;
      const groupSizes = splitConnectionIntoGroups(conn.baseSize, conn.residenceId, conn.jobId);
      groupSizes.forEach((groupSize) => {
        const id = (connectionCounter++).toString();
        if (finalNeighborhoods[conn.jobId]) {
          finalNeighborhoods[conn.jobId].popIds.push(id);
        }
        if (finalNeighborhoods[conn.residenceId]) {
          finalNeighborhoods[conn.residenceId].popIds.push(id);
        }
        neighborhoodConnections.push({
          residenceId: conn.residenceId,
          jobId: conn.jobId,
          size: groupSize,
          drivingDistance: conn.drivingDistance,
          drivingSeconds: conn.drivingSeconds,
          id,
        });
      });
    });
  });

  return {
    points: Object.values(finalNeighborhoods),
    pops: neighborhoodConnections,
  }
};

const processBuildings = (place, rawBuildings) => {
  // looking at the sample data, cells are approximately 100 meters long and wide, so thats what im gonna go with
  let minLon = 9999;
  let minLat = 9999;
  let maxLon = -999;
  let maxLat = -999;

  let processedBuildings = {};

  rawBuildings.forEach((building, i) => {
    let minBuildingLon = 9999;
    let minBuildingLat = 9999;
    let maxBuildingLon = -999;
    let maxBuildingLat = -999;

    const __points = building.geometry.map((coord) => {
      // overall bbox
      if (coord.lon < minLon) minLon = coord.lon;
      if (coord.lat < minLat) minLat = coord.lat;
      if (coord.lon > maxLon) maxLon = coord.lon;
      if (coord.lat > maxLat) maxLat = coord.lat;

      // building bbox
      if (coord.lon < minBuildingLon) minBuildingLon = coord.lon;
      if (coord.lat < minBuildingLat) minBuildingLat = coord.lat;
      if (coord.lon > maxBuildingLon) maxBuildingLon = coord.lon;
      if (coord.lat > maxBuildingLat) maxBuildingLat = coord.lat;

      return [coord.lon, coord.lat];
    });
    if (__points.length < 3) return;
    if (__points[0][0] !== __points[__points.length - 1][0] || __points[0][1] !== __points[__points.length - 1][1]) __points.push(__points[0]);
    const buildingPolygon = turf.polygon([__points]);
    const buildingCenter = turf.centerOfMass(buildingPolygon);

    processedBuildings[i] = {
      bbox: {
        minLon: minBuildingLon,
        maxLat: minBuildingLat,
        maxLon: maxBuildingLon,
        maxLat: maxBuildingLat,
      },
      center: buildingCenter.geometry.coordinates,
      ...building,
      id: i,
      geometry: buildingPolygon.geometry.coordinates,
    }
  });

  // creating the grid of cells
  // creating two lines that we will use to metaphorically split up the bbox
  const verticalLine = turf.lineString([[minLon, minLat], [minLon, maxLat]]);
  const horizontalLine = turf.lineString([[minLon, minLat], [maxLon, minLat]]);

  const verticalLength = turf.length(verticalLine, { units: 'meters' });
  const horizontalLength = turf.length(horizontalLine, { units: 'meters' });

  let columnCoords = []; // x, made by going along horizontal line
  let rowCoords = []; // y, made by going along vertical line

  let cellSize = null;

  // generating the column coords
  for (let x = 0; x <= horizontalLength; x += 100) {
    const minX = turf.along(horizontalLine, x, { units: 'meters' }).geometry.coordinates[0];
    columnCoords.push(minX); //minX will be inclusive
  };

  // generating the row coords
  for (let y = 0; y <= verticalLength; y += 100) {
    const minY = turf.along(verticalLine, y, { units: 'meters' }).geometry.coordinates[1];

    if (y == 100) cellSize = Number((minY - rowCoords[0]).toFixed(4)); // idk why we need this, but its in the data format!

    rowCoords.push(minY); //minY will be inclusive
  };

  // figuring out what buildings are in what cells

  // longitude/x
  for (let x = 0; x < columnCoords.length; x++) {
    const thisMinLon = columnCoords[x];
    const nextMinLon = x == columnCoords.length - 1 ? 999 : columnCoords[x + 1]; // if in the last column, "next" min longitude value is just big

    const buildingsThatFit = Object.values(processedBuildings).filter((building) => thisMinLon <= building.center[0] && nextMinLon > building.center[0]);
    buildingsThatFit.forEach((building) => {
      processedBuildings[building.id].xCellCoord = x;
    })
  };

  // latitude/y
  for (let y = 0; y < rowCoords.length; y++) {
    const thisMinLon = rowCoords[y];
    const nextMinLon = y == rowCoords.length - 1 ? rowCoords[y] + cellSize : rowCoords[y + 1]; // if in the last row, "next" min latitude value is just big

    if (y == rowCoords.length - 1) { // adjusting the maxLat to fix the grid
      maxLat = rowCoords[y] + cellSize;
    }

    const buildingsThatFit = Object.values(processedBuildings).filter((building) => thisMinLon <= building.center[1] && nextMinLon > building.center[1]);
    buildingsThatFit.forEach((building) => {
      processedBuildings[building.id].yCellCoord = y;
    })
  };

  // building the dictionary of cells, finally
  let cellsDict = {};
  Object.values(processedBuildings).forEach((building) => {
    const buildingCoord = `${building.xCellCoord},${building.yCellCoord}`;
    if (!cellsDict[buildingCoord]) cellsDict[buildingCoord] = [];
    cellsDict[buildingCoord].push(building.id);
  });

  let maxDepth = 1;

  const optimizedIndex = optimizeIndex({
    cellHeightCoords: cellSize,
    minLon,
    minLat,
    maxLon,
    maxLat,
    cols: columnCoords.length,
    rows: rowCoords.length,
    cells: cellsDict,
    buildings: Object.values(processedBuildings).map((building) => {
      if (
        building.tags['building:levels:underground'] &&
        Number(building.tags['building:levels:underground']) > maxDepth
      )
        maxDepth = Number(building.tags['building:levels:underground']);

      return {
        minX: building.bbox.minLon,
        minY: building.bbox.minLat,
        maxX: building.bbox.maxLon,
        maxY: building.bbox.maxLat,
        foundationDepth: building.tags['building:levels:underground'] ? Number(building.tags['building:levels:underground']) : 1,
        polygon: building.geometry,
      }
    }),
    maxDepth,
  });

  return optimizedIndex;
}

const shouldKeepRoadFeature = (feature) => {
  if (!feature?.geometry) return false;
  if (ROAD_FILTERS.excludeHighways?.size) {
    const highwayType = feature.properties?.highway;
    if (highwayType && ROAD_FILTERS.excludeHighways.has(highwayType)) {
      return false;
    }
  }

  if (ROAD_FILTERS.minLengthMeters && ROAD_FILTERS.minLengthMeters > 0) {
    try {
      if (feature.geometry.type === 'LineString' || feature.geometry.type === 'MultiLineString') {
        const length = turf.length(feature, { units: 'meters' });
        if (length < ROAD_FILTERS.minLengthMeters) return false;
      }
    } catch (err) {
      // ignore length calculation issues; keep the feature
    }
  }

  return true;
};

const processRoads = (place) => {
  const roadFilePath = `./raw_data/${place.code}/roads.geojson`;
  if (!fs.existsSync(roadFilePath)) return null;

  const roadGeoJson = JSON.parse(fs.readFileSync(roadFilePath, 'utf8'));
  if (!roadGeoJson?.features) return roadGeoJson;

  roadGeoJson.features = roadGeoJson.features.filter((feature) => shouldKeepRoadFeature(feature));

  return roadGeoJson;
};

const processAllData = async (place) => {
  const readJsonFile = (filePath) => {
    return new Promise((resolve, reject) => {
      const parseStream = createParseStream();
      let jsonData;

      parseStream.on('data', (data) => {
        jsonData = data;
      });

      parseStream.on('end', () => {
        resolve(jsonData);
      });

      parseStream.on('error', (err) => {
        reject(err);
      });

      fs.createReadStream(filePath).pipe(parseStream);
    });
  };

  console.log('Reading raw data for', place.code);
  const rawBuildings = await readJsonFile(`./raw_data/${place.code}/buildings.json`);
  const rawPlaces = await readJsonFile(`./raw_data/${place.code}/places.json`);

  console.log('Processing Buildings for', place.code)
  const processedBuildings = processBuildings(place, rawBuildings);
  console.log('Processing Connections/Demand for', place.code)
  const processedConnections = processPlaceConnections(place, rawBuildings, rawPlaces);
  console.log('Processing Roads for', place.code)
  const processedRoads = processRoads(place);

  console.log('Writing finished data for', place.code)
  writeJson(`./processed_data/${place.code}/buildings_index.json`, processedBuildings);
  if (processedRoads) {
    writeJson(`./processed_data/${place.code}/roads.geojson`, processedRoads);
  }
  writeJson(`./processed_data/${place.code}/demand_data.json`, processedConnections);
};

if (!fs.existsSync('./processed_data')) fs.mkdirSync('./processed_data');
config.places.forEach((place) => {
  (async () => {
    if (fs.existsSync(`./processed_data/${place.code}`)) fs.rmSync(`./processed_data/${place.code}`, { recursive: true, force: true });
    fs.mkdirSync(`./processed_data/${place.code}`)
    await processAllData(place);
    console.log(`Finished processing ${place.code}.`);
  })();
});
