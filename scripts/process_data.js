import fs from 'fs';
import config from '../config.js';
import * as turf from '@turf/turf';
import { createParseStream } from 'big-json';

let terminalTicker = 0;

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

const DISTANCE_WEIGHTING = {
  smallThreshold: Number.isFinite(config.distanceWeighting?.smallThreshold)
    ? config.distanceWeighting.smallThreshold
    : 300,
  mediumThreshold: Number.isFinite(config.distanceWeighting?.mediumThreshold)
    ? config.distanceWeighting.mediumThreshold
    : 1200,
  largeThreshold: Number.isFinite(config.distanceWeighting?.largeThreshold)
    ? config.distanceWeighting.largeThreshold
    : 4000,
  megaThreshold: Number.isFinite(config.distanceWeighting?.megaThreshold)
    ? config.distanceWeighting.megaThreshold
    : 10000,
  smallScaleKm: Number.isFinite(config.distanceWeighting?.smallScaleKm)
    ? config.distanceWeighting.smallScaleKm
    : 1.6,
  mediumScaleKm: Number.isFinite(config.distanceWeighting?.mediumScaleKm)
    ? config.distanceWeighting.mediumScaleKm
    : 2.5,
  largeScaleKm: Number.isFinite(config.distanceWeighting?.largeScaleKm)
    ? config.distanceWeighting.largeScaleKm
    : 3.8,
  megaScaleKm: Number.isFinite(config.distanceWeighting?.megaScaleKm)
    ? config.distanceWeighting.megaScaleKm
    : 6,
  extremeScaleKm: Number.isFinite(config.distanceWeighting?.extremeScaleKm)
    ? config.distanceWeighting.extremeScaleKm
    : 8.5,
  distanceExponent: Number.isFinite(config.distanceWeighting?.distanceExponent)
    ? config.distanceWeighting.distanceExponent
    : 1.8,
  jobExponent: Number.isFinite(config.distanceWeighting?.jobExponent)
    ? config.distanceWeighting.jobExponent
    : 0.55,
  clusterExponent: Number.isFinite(config.distanceWeighting?.clusterExponent)
    ? config.distanceWeighting.clusterExponent
    : 0.35,
  minDistanceKm: Number.isFinite(config.distanceWeighting?.minDistanceKm)
    ? config.distanceWeighting.minDistanceKm
    : 0.3,
  closenessFloor: Number.isFinite(config.distanceWeighting?.closenessFloor)
    ? config.distanceWeighting.closenessFloor
    : 0.005,
  closenessPower: Number.isFinite(config.distanceWeighting?.closenessPower)
    ? config.distanceWeighting.closenessPower
    : 1.1,
  terminalWeightMultiplier: Number.isFinite(config.distanceWeighting?.terminalWeightMultiplier)
    ? config.distanceWeighting.terminalWeightMultiplier
    : 1.2,
  terminalClosenessExponent: Number.isFinite(config.distanceWeighting?.terminalClosenessExponent)
    ? config.distanceWeighting.terminalClosenessExponent
    : 1.1,
  baseWeight: Number.isFinite(config.distanceWeighting?.baseWeight)
    ? config.distanceWeighting.baseWeight
    : 0.02,
  terminalMaxShare: Number.isFinite(config.distanceWeighting?.terminalMaxShare)
    ? config.distanceWeighting.terminalMaxShare
    : 0.01,
  terminalMinShare: Number.isFinite(config.distanceWeighting?.terminalMinShare)
    ? config.distanceWeighting.terminalMinShare
    : 0.0003,
  globalTerminalShare: Number.isFinite(config.distanceWeighting?.globalTerminalShare)
    ? config.distanceWeighting.globalTerminalShare
    : 0.004,
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

const distanceScaleForPopulation = (population) => {
  if (population < DISTANCE_WEIGHTING.smallThreshold) return DISTANCE_WEIGHTING.smallScaleKm;
  if (population < DISTANCE_WEIGHTING.mediumThreshold) return DISTANCE_WEIGHTING.mediumScaleKm;
  if (population < DISTANCE_WEIGHTING.largeThreshold) return DISTANCE_WEIGHTING.largeScaleKm;
  if (population < DISTANCE_WEIGHTING.megaThreshold) return DISTANCE_WEIGHTING.megaScaleKm;
  return DISTANCE_WEIGHTING.extremeScaleKm;
};

const normalizeIataCode = (value) => {
  if (!value) return null;
  const cleaned = value.toString().trim().toUpperCase();
  if (!cleaned) return null;
  if (/^[A-Z0-9]{3}$/u.test(cleaned)) return cleaned;
  if (/^[A-Z0-9]{4}$/u.test(cleaned)) return cleaned.slice(0, 3);
  return null;
};

const extractIataCode = (tags = {}) => {
  const candidates = [
    tags.iata,
    tags['ref:iata'],
    tags['iata:code'],
    tags['airport:iata'],
    tags.ref,
  ];
  for (const candidate of candidates) {
    const normalized = normalizeIataCode(candidate);
    if (normalized) return normalized;
  }
  return null;
};

const findNearestAerodrome = (centerCoords, aerodromes) => {
  if (!aerodromes.length) return null;
  const centerPoint = turf.point(centerCoords);
  let best = null;
  let bestDistance = Infinity;
  aerodromes.forEach((aerodrome) => {
    if (!aerodrome.center) return;
    const distance = turf.distance(centerPoint, turf.point(aerodrome.center), { units: 'kilometers' });
    if (distance < bestDistance) {
      bestDistance = distance;
      best = { ...aerodrome, distance };
    }
  });
  if (best && best.distance <= 80) return best;
  return null;
};

const determineTerminalLabel = (placeFeature, centerCoords, aerodromes, terminalNameCountersRef) => {
  const tags = placeFeature.tags ?? {};
  let code = extractIataCode(tags);
  let airportName = tags.name || tags['name:en'];

  if (!code || !airportName) {
    const nearest = findNearestAerodrome(centerCoords, aerodromes);
    if (nearest) {
      if (!code) code = extractIataCode(nearest.tags);
      if (!airportName) airportName = nearest.tags?.name || nearest.tags?.['name:en'];
    }
  }

  const baseKey = code ?? airportName ?? 'Terminal';
  terminalNameCountersRef[baseKey] = (terminalNameCountersRef[baseKey] || 0) + 1;
  const sequence = terminalNameCountersRef[baseKey];
  const internalId = `AIR_Terminal_${terminalTicker}`;
  terminalTicker += 1;

  let name = null;
  if (code) {
    name = sequence > 1 ? `${code} Terminal ${sequence}` : `${code} Terminal 1`;
  } else if (airportName) {
    if (/terminal/i.test(airportName)) {
      name = sequence > 1 ? `${airportName} ${sequence}` : airportName;
    } else {
      name = `${airportName} Terminal ${sequence}`;
    }
  } else {
    name = `Terminal ${sequence}`;
  }

  return {
    internalId,
    name,
    iata: code || null,
    groupKey: baseKey,
    sequence,
  };
};

const computeConnectionMetrics = (outerPlace, innerPlace, distanceMeters) => {
  const distanceKm = Math.max(DISTANCE_WEIGHTING.minDistanceKm, distanceMeters / 1000);
  const distanceScale = distanceScaleForPopulation(outerPlace.totalPopulation);
  const closenessRaw = 1 / (1 + Math.pow(distanceKm / distanceScale, DISTANCE_WEIGHTING.distanceExponent));
  let closeness = Math.max(
    DISTANCE_WEIGHTING.closenessFloor,
    closenessRaw
  );
  const jobScore = Math.pow(innerPlace.totalJobs + 1, DISTANCE_WEIGHTING.jobExponent);
  const clusterBoost = Math.pow(
    Math.max(innerPlace.percentOfTotalJobs, 1e-6),
    DISTANCE_WEIGHTING.clusterExponent
  );
  let closenessPower = DISTANCE_WEIGHTING.closenessPower;
  let weight = jobScore * clusterBoost;

  if (innerPlace.isTerminal) {
    closenessPower = Math.min(closenessPower, DISTANCE_WEIGHTING.terminalClosenessExponent);
    weight *= DISTANCE_WEIGHTING.terminalWeightMultiplier;
  }

  const adjustedWeight = weight * Math.pow(closeness, closenessPower) + DISTANCE_WEIGHTING.baseWeight;

  return {
    weight: adjustedWeight,
    closeness,
  };
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
  yes: 450, // most likely a SFH
  apartments: 450,
  barracks: 150, // google said 70-90, but imma bump it up a bit tbh
  bungalow: 750, // sfh
  cabin: 750, // sfh
  detached: 750, // sfh
  annexe: 600, // kinda like apartments
  dormitory: 200, // good lord
  farm: 800, // sfh
  ger: 300, // technically sfh, but generally usually smaller and more compact. honorary apartment. TIL "ger" is mongolian for the english word "yurt"
  hotel: 350, // gonna count these as apartments because hotel guests use transit too
  house: 750, // sfh
  houseboat: 650, // interesting
  residential: 450, // could be anything, but let's align with apartments
  semidetached_house: 550, // duplex
  static_caravan: 500,
  stilt_house: 800,
  terrace: 550, // townhome
  tree_house: 300, // fuck it
  trullo: 300, // there is nothing scientific here, its all fucking vibes
};

const squareFeetPerJob = {
  commercial: 350, // non specific, restaraunts i guess?
  industrial: 400, // vibes vibes vibes vibes!!!!!,
  kiosk: 150, // its all vibes baby
  office: 175, // all of my vibes are 100% meat created
  retail: 500,
  supermarket: 300,
  warehouse: 600,
  // the following are all religious and im assuming ~100 square feet, not for job purposes, 
  // but for the fact that people go to religious institutions
  // might use a similar trick for sports stadiums
  religious: 200,
  cathedral: 200,
  chapel: 200,
  church: 200,
  kingdom_hall: 200,
  monastery: 200,
  mosque: 200,
  presbytery: 200,
  shrine: 200,
  synagogue: 200,
  temple: 200,
  // end of religious
  bakehouse: 300,
  college: 200, // collge/uni is a job
  fire_station: 400,
  government: 175,
  gatehouse: 200,
  hospital: 125,
  kindergarten: 200,
  museum: 300,
  public: 300,
  school: 175,
  train_station: 1000,
  transportation: 1000,
  university: 200,
  // sports time! im going to treat these like offices because i said so.
  // i think itll end up creating demand thats on average what stadiums see traffic wise. not sure
  grandstand: 200,
  pavilion: 200,
  riding_hall: 200,
  sports_hall: 200,
  sports_centre: 200,
  stadium: 200,
};

const processPlaceConnections = (place, rawBuildings, rawPlaces) => {
  let neighborhoods = {};
  let centersOfNeighborhoods = {};
  let calculatedBuildings = {};

  const basePlaceTypes = new Set(['quarter', 'neighbourhood']);
  const sprawlPlaceTypes = new Set(['suburb', 'town', 'village', 'hamlet', 'isolated_dwelling', 'locality', 'residential', 'city']);
  const ruralCandidates = [];
  const terminalPlaces = [];
  const aerodromePlaces = [];
  const terminalLabelMapping = {};
  const terminalMetaByPlace = {};
  const terminalMetaByDisplay = {};
  const terminalNameCounters = {};
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
  const orphanPopulationThreshold = Number.isFinite(config.orphanClustering?.minPopulation)
    ? config.orphanClustering.minPopulation
    : 30;
  const orphanAdoptionThreshold = Number.isFinite(config.orphanClustering?.adoptionThresholdKm)
    ? config.orphanClustering.adoptionThresholdKm
    : 0.9; // km
  const orphanBaseDistanceKm = Number.isFinite(config.orphanClustering?.baseDistanceKm)
    ? config.orphanClustering.baseDistanceKm
    : 1.2;
  const orphanMinPoints = Number.isFinite(config.orphanClustering?.minPoints)
    ? config.orphanClustering.minPoints
    : 4;
  const orphanMaxDistanceKm = Number.isFinite(config.orphanClustering?.maxDistanceKm)
    ? config.orphanClustering.maxDistanceKm
    : 1.6;
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
    const placeType = placeFeature.tags?.place;
    const isTerminal = placeFeature.tags?.aeroway === 'terminal';
    const isAerodrome = placeFeature.tags?.aeroway === 'aerodrome';

    if (isTerminal) {
      const terminalFeature = {
        ...placeFeature,
        tags: {
          ...placeFeature.tags,
          'subwaybuilder:is_terminal': 'true',
        },
      };
      terminalPlaces.push(terminalFeature);
      return;
    }

    if (isAerodrome) {
      const center = computePlaceCenter(placeFeature);
      if (center) {
        aerodromePlaces.push({
          ...placeFeature,
          center,
        });
      }
      return;
    }

    if (placeType && basePlaceTypes.has(placeType)) {
      directPlaces.push(placeFeature);
      return;
    }

    if (placeType && sprawlPlaceTypes.has(placeType)) {
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
        let approxJobs = Math.floor(buildingArea / squareFeetPerJob[building.tags.building]);

        if (building.tags.aeroway && building.tags.aeroway === 'terminal') {
          approxJobs = Math.max(Math.floor(buildingArea / 320) * 3, 120);
        }

        calculatedBuildings[building.id] = {
          ...building,
          approxJobs,
          buildingCenter,
        };
      } else if (building.tags.aeroway === 'terminal') {
        const approxJobs = Math.max(Math.floor(buildingArea / 320) * 3, 120);
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
    const isTerminal = placeFeature.tags?.aeroway === 'terminal';

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

    const radius = isTerminal
      ? 1.2
      : (ruralRadiusByPlaceType[placeType] ?? 2);
    const centerPoint = turf.point(center);
    return residentialBuildingPoints.features.filter((feature) => turf.distance(centerPoint, feature, { units: 'kilometers' }) <= radius);
  };

  let syntheticClusterCounter = 0;
  const coveredBuildingIDs = new Set();

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
    return syntheticId;
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
      placeBuildings.forEach((feature) => {
        coveredBuildingIDs.add(feature.properties.buildingID);
      });
      return;
    }

    finalGroups.forEach((finalGroup, index) => {
      const centerPoint = turf.centerOfMass(turf.featureCollection(finalGroup));
      const useSuffix = finalGroups.length > 1;
      const clusterLabel = useSuffix ? `Cluster ${index + 1}` : null;

      const centerId = addCenterFromFeature(
        placeFeature,
        centerPoint.geometry.coordinates,
        clusterLabel
      );
      finalGroup.forEach((feature) => {
        coveredBuildingIDs.add(feature.properties.buildingID);
      });
    });
  };

  directPlaces.forEach((placeFeature) => clusterPlaceFeature(placeFeature));
  ruralCandidates.forEach((placeFeature) => clusterPlaceFeature(placeFeature));
  terminalPlaces.forEach((placeFeature) => {
    const center = computePlaceCenter(placeFeature);
    if (!center) return;
    const terminalInfo = determineTerminalLabel(placeFeature, center, aerodromePlaces, terminalNameCounters);
    const terminalId = addCenterFromFeature(placeFeature, center);
    if (terminalId) {
      terminalLabelMapping[terminalId] = terminalInfo;
      if (placeFeature.id) terminalLabelMapping[placeFeature.id.toString()] = terminalInfo;
      terminalMetaByPlace[placeFeature.id?.toString?.() ?? terminalId] = terminalInfo;
      if (!neighborhoods[terminalId].tags) neighborhoods[terminalId].tags = {};
      neighborhoods[terminalId].tags.name = terminalInfo.name;
      if (terminalInfo.iata) neighborhoods[terminalId].tags.iata = terminalInfo.iata;
      neighborhoods[terminalId].tags['subwaybuilder:is_terminal'] = 'true';
      neighborhoods[terminalId].id = terminalInfo.internalId;
      terminalMetaByDisplay[terminalInfo.internalId] = terminalInfo;
    }
    const nearbyResidential = getBuildingsForPlace(placeFeature);
    nearbyResidential.forEach((feature) => coveredBuildingIDs.add(feature.properties.buildingID));
  });

  const centerPoints = Object.entries(centersOfNeighborhoods).map(([placeID, coords]) =>
    turf.point(coords, { placeID })
  );

  const addSyntheticOrphanClusters = () => {
    if (!residentialBuildingPoints.features.length) return;

    const orphanCellSizeDegrees = Number.isFinite(config.orphanClustering?.cellSizeDegrees)
      ? config.orphanClustering.cellSizeDegrees
      : 0.004; // roughly 300-400m at mid-latitudes

    const cellBuckets = {};

    residentialBuildingPoints.features.forEach((feature) => {
      if (coveredBuildingIDs.has(feature.properties.buildingID)) return;
      const [lon, lat] = feature.geometry.coordinates;
      const lonKey = Math.floor(lon / orphanCellSizeDegrees);
      const latKey = Math.floor(lat / orphanCellSizeDegrees);
      const cellKey = `${lonKey}:${latKey}`;
      if (!cellBuckets[cellKey]) {
        cellBuckets[cellKey] = {
          features: [],
          population: 0,
          minLon: lon,
          minLat: lat,
          maxLon: lon,
          maxLat: lat,
        };
      }
      const cell = cellBuckets[cellKey];
      cell.features.push(feature);
      const approxPop = feature.properties?.approxPop ?? 0;
      cell.population += approxPop;
      if (lon < cell.minLon) cell.minLon = lon;
      if (lat < cell.minLat) cell.minLat = lat;
      if (lon > cell.maxLon) cell.maxLon = lon;
      if (lat > cell.maxLat) cell.maxLat = lat;
    });

    Object.values(cellBuckets)
      .filter((cell) => cell.population >= orphanPopulationThreshold && cell.features.length)
      .forEach((cell) => {
        const cellFeatureCollection = turf.featureCollection(cell.features);

        let cellDiagonalKm = turf.distance(
          turf.point([cell.minLon, cell.minLat]),
          turf.point([cell.maxLon, cell.maxLat]),
          { units: 'kilometers' }
        );
        if (!Number.isFinite(cellDiagonalKm) || cellDiagonalKm === 0) cellDiagonalKm = orphanBaseDistanceKm;

        const clusterDistance = Math.min(
          orphanMaxDistanceKm,
          Math.max(orphanBaseDistanceKm, cellDiagonalKm * 1.5)
        );

        let clustered;
        try {
          clustered = turf.clustersDbscan(
            cellFeatureCollection,
            clusterDistance,
            {
              minPoints: Math.min(orphanMinPoints, cell.features.length),
              units: 'kilometers',
            }
          );
        } catch (err) {
          clustered = { features: cell.features.map((feature) => ({ ...feature, properties: { ...feature.properties, cluster: 0 } })) };
        }

        const groups = organizeDbscanGroups(clustered.features)
          .filter((group) => group.length);

        groups.forEach((group) => {
          const groupPopulation = group.reduce((sum, feature) => sum + (feature.properties?.approxPop ?? 0), 0);
          if (groupPopulation < orphanPopulationThreshold) return;

          const clusterCenter = turf.centerOfMass(turf.featureCollection(group));
          let minDistance = Infinity;
          if (centerPoints.length) {
            centerPoints.forEach((centerFeature) => {
              const distance = turf.distance(clusterCenter, centerFeature, { units: 'kilometers' });
              if (distance < minDistance) minDistance = distance;
            });
            if (minDistance <= orphanAdoptionThreshold) return;
          }

          const derivedName = deriveNameFromBuildings(group);
          const syntheticFeature = {
            tags: {
              name: derivedName ?? 'Synthetic Cluster',
              'subwaybuilder:synthetic': 'true',
              'subwaybuilder:cluster-source': 'orphan-density',
            },
          };
          const centerId = addCenterFromFeature(syntheticFeature, clusterCenter.geometry.coordinates, null);
          centerPoints.push(turf.point(clusterCenter.geometry.coordinates, { placeID: centerId }));
          group.forEach((feature) => {
            coveredBuildingIDs.add(feature.properties.buildingID);
          });
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
      residentCentroid: null,
      jobCentroid: null,
    };
    let residentWeightLon = 0;
    let residentWeightLat = 0;
    let residentWeightTotal = 0;
    let jobWeightLon = 0;
    let jobWeightLat = 0;
    let jobWeightTotal = 0;

    buildingsWhichExistWithinFeature.features.forEach((feature) => {
      const building = calculatedBuildings[feature.properties.buildingID];
      const [lon, lat] = building.buildingCenter;
      const approxPop = building.approxPop ?? 0;
      const approxJobs = building.approxJobs ?? 0;
      finalFeature.totalPopulation += approxPop;
      finalFeature.totalJobs += approxJobs;
      totalPopulation += approxPop;
      totalJobs += approxJobs;
      if (approxPop > 0) {
        residentWeightLon += lon * approxPop;
        residentWeightLat += lat * approxPop;
        residentWeightTotal += approxPop;
      }
      if (approxJobs > 0) {
        jobWeightLon += lon * approxJobs;
        jobWeightLat += lat * approxJobs;
        jobWeightTotal += approxJobs;
      }
    });

    if (residentWeightTotal > 0) {
      finalFeature.residentCentroid = [
        residentWeightLon / residentWeightTotal,
        residentWeightLat / residentWeightTotal,
      ];
    }
    if (jobWeightTotal > 0) {
      finalFeature.jobCentroid = [
        jobWeightLon / jobWeightTotal,
        jobWeightLat / jobWeightTotal,
      ];
    }

    const basePlaceTags = neighborhoods[feature.properties.placeID]?.tags;
    finalFeature.isTerminal = !!(basePlaceTags && (basePlaceTags.aeroway === 'terminal' || basePlaceTags['subwaybuilder:is_terminal'] === 'true'));

    finalVoronoiMetadata[feature.properties.placeID] = finalFeature;
  });

  const combinedNeighborhoods = {};
  const combinedIdMap = {};
  const combinedIdMapReverse = {};
  let neighborhoodConnections = [];

  // creating total percents and setting up final dicts
  Object.values(finalVoronoiMetadata).forEach((place) => {
    finalVoronoiMetadata[place.placeID].percentOfTotalPopulation = place.totalPopulation / totalPopulation;
    finalVoronoiMetadata[place.placeID].percentOfTotalJobs = place.totalJobs / totalJobs;

    const baseCenter = centersOfNeighborhoods[place.placeID];

    const basePlace = neighborhoods[place.placeID];
    const baseTags = basePlace?.tags ? { ...basePlace.tags } : {};

    const isTerminal = !!(baseTags.aeroway === 'terminal' || baseTags['subwaybuilder:is_terminal'] === 'true');

    let outputId = place.placeID;
    const outputTags = { ...baseTags };
    outputTags['subwaybuilder:cluster-kind'] = 'mixed';

    if (isTerminal) {
      outputTags['subwaybuilder:is_terminal'] = 'true';
      const mappedInfo = terminalMetaByPlace[place.placeID] ?? terminalLabelMapping[place.placeID] ?? terminalLabelMapping[`${place.placeID}`];
      if (mappedInfo) {
        outputId = mappedInfo.internalId;
        outputTags.name = mappedInfo.name;
        if (mappedInfo.iata) outputTags.iata = mappedInfo.iata;
        terminalMetaByDisplay[outputId] = mappedInfo;
        terminalMetaByPlace[place.placeID] = mappedInfo;
      } else {
        outputTags.name = outputTags.name || `Terminal ${terminalTicker}`;
      }
    } else if (outputTags.aeroway) {
      delete outputTags.aeroway;
    }

    if (!outputTags.name && baseTags?.name) outputTags.name = baseTags.name;

    const location = place.jobCentroid ?? place.residentCentroid ?? baseCenter;
    const jobDominant = !isTerminal && place.totalJobs >= place.totalPopulation * 5;
    const jobsValue = place.totalJobs;
    const residentsValue = (isTerminal || jobDominant) ? 0 : place.totalPopulation;

    combinedNeighborhoods[outputId] = {
      id: outputId,
      location,
      jobs: jobsValue,
      residents: residentsValue,
      popIds: [],
      source: place.placeID,
      tags: outputTags,
    };
    combinedIdMap[place.placeID] = outputId;
    combinedIdMapReverse[outputId] = place.placeID;
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
        const { weight, closeness } = computeConnectionMetrics(outerPlace, innerPlace, connectionDistance);
        return {
          residenceId: outerPlace.placeID,
          jobId: innerPlace.placeID,
          weight,
          closeness,
          isTerminal: innerPlace.isTerminal,
          distanceMeters: connectionDistance,
          drivingDistance: Math.round(connectionDistance),
          drivingSeconds: Math.round(connectionDistance * 0.12),
        };
      });

    if (!connectionCandidates.length) return;

    const maxConnections = POPULATION_GROUP_CONFIG.maxConnectionsPerPoint
      ? Math.min(POPULATION_GROUP_CONFIG.maxConnectionsPerPoint, connectionCandidates.length)
      : connectionCandidates.length;

    const localQuota = (() => {
      if (maxConnections <= 0) return 0;
      if (totalPop < DISTANCE_WEIGHTING.smallThreshold) return Math.min(4, maxConnections);
      if (totalPop < DISTANCE_WEIGHTING.mediumThreshold) return Math.min(3, maxConnections);
      if (totalPop < DISTANCE_WEIGHTING.largeThreshold) return Math.min(2, maxConnections);
      return Math.min(2, maxConnections);
    })();

    const byDistance = [...connectionCandidates].sort((a, b) => a.distanceMeters - b.distanceMeters);
    const byWeight = [...connectionCandidates].sort((a, b) => b.weight - a.weight);
    const selectedMap = new Map();

    byDistance.slice(0, localQuota).forEach((candidate) => {
      if (!selectedMap.has(candidate.jobId)) selectedMap.set(candidate.jobId, candidate);
    });

    const terminalCandidates = connectionCandidates.filter((candidate) => candidate.isTerminal);
    terminalCandidates.forEach((candidate) => {
      if (!selectedMap.has(candidate.jobId)) selectedMap.set(candidate.jobId, candidate);
    });

    const effectiveMaxConnections = Math.max(maxConnections, selectedMap.size);

    for (const candidate of byWeight) {
      if (selectedMap.size >= effectiveMaxConnections) break;
      if (!selectedMap.has(candidate.jobId)) selectedMap.set(candidate.jobId, candidate);
    }

    let limitedCandidates = Array.from(selectedMap.values());

    const computeWeights = (candidates) => {
      let total = 0;
      let terminal = 0;
      candidates.forEach((candidate) => {
        total += candidate.weight;
        const originalId = combinedIdMapReverse[candidate.jobId];
        if (originalId && finalVoronoiMetadata[originalId]?.isTerminal) {
          terminal += candidate.weight;
          candidate.isTerminal = true;
        } else {
          candidate.isTerminal = false;
        }
      });
      return { total, terminal };
    };

    let { total: totalWeight, terminal: terminalWeight } = computeWeights(limitedCandidates);

    if (terminalWeight > 0 && totalWeight > 0) {
      const currentShare = terminalWeight / totalWeight;
      if (currentShare > DISTANCE_WEIGHTING.terminalMaxShare || currentShare < DISTANCE_WEIGHTING.terminalMinShare) {
        const targetShare = Math.min(
          DISTANCE_WEIGHTING.terminalMaxShare,
          Math.max(DISTANCE_WEIGHTING.terminalMinShare, currentShare)
        );
        const desiredTerminalWeight = totalWeight * targetShare;
        const scaleFactor = desiredTerminalWeight / terminalWeight;
        limitedCandidates = limitedCandidates.map((candidate) => {
          if (!candidate.isTerminal) return candidate;
          return { ...candidate, weight: Math.max(candidate.weight * scaleFactor, DISTANCE_WEIGHTING.baseWeight / 10) };
        });
        ({ total: totalWeight, terminal: terminalWeight } = computeWeights(limitedCandidates));
      }
    }

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

    rawConnections.sort((a, b) => (b.remainder * b.closeness) - (a.remainder * a.closeness));

    for (let i = 0; remaining > 0 && rawConnections.length > 0; i++, remaining--) {
      rawConnections[i % rawConnections.length].baseSize += 1;
    }

    const isTerminalJob = (jobId) => {
      const originalId = combinedIdMapReverse[jobId];
      return originalId && finalVoronoiMetadata[originalId]?.isTerminal;
    };
    const terminalConnections = rawConnections.filter((conn) => isTerminalJob(conn.jobId));
    const nonTerminalConnections = rawConnections.filter((conn) => !isTerminalJob(conn.jobId));

    if (terminalConnections.length && nonTerminalConnections.length) {
      const maxTerminalTrips = Math.max(1, Math.round(totalPop * DISTANCE_WEIGHTING.terminalMaxShare));
      const minTerminalTrips = Math.round(totalPop * DISTANCE_WEIGHTING.terminalMinShare);

      let terminalTotal = terminalConnections.reduce((sum, conn) => sum + conn.baseSize, 0);

      if (terminalTotal > maxTerminalTrips) {
        let over = terminalTotal - maxTerminalTrips;
        const sortedTerminals = [...terminalConnections].sort((a, b) => a.closeness - b.closeness);
        let released = 0;
        for (const conn of sortedTerminals) {
          if (over <= 0) break;
          const reducible = Math.min(conn.baseSize, over);
          if (reducible <= 0) continue;
          conn.baseSize -= reducible;
          over -= reducible;
          released += reducible;
        }
        if (released > 0) {
          const sortedNonTerminals = [...nonTerminalConnections].sort((a, b) => b.closeness - a.closeness);
          let idx = 0;
          while (released > 0 && sortedNonTerminals.length) {
            sortedNonTerminals[idx % sortedNonTerminals.length].baseSize += 1;
            released -= 1;
            idx += 1;
          }
        }
        terminalTotal = terminalConnections.reduce((sum, conn) => sum + conn.baseSize, 0);
      }

      if (terminalTotal < minTerminalTrips) {
        let needed = minTerminalTrips - terminalTotal;
        if (needed > 0) {
          const sortedNonTerminals = [...nonTerminalConnections].sort((a, b) => a.closeness - b.closeness);
          const sortedTerminals = [...terminalConnections].sort((a, b) => b.closeness - a.closeness);
          let idx = 0;
          while (needed > 0 && sortedNonTerminals.length) {
            const donor = sortedNonTerminals[idx % sortedNonTerminals.length];
            if (donor.baseSize <= 1) {
              idx += 1;
              if (idx > sortedNonTerminals.length * 2) break;
              continue;
            }
            donor.baseSize -= 1;
            sortedTerminals[idx % sortedTerminals.length].baseSize += 1;
            needed -= 1;
            idx += 1;
          }
        }
      }
    }

    if (POPULATION_GROUP_CONFIG.minimumFinalizeSize) {
      const nearestSelected = byDistance
        .filter((candidate) => selectedMap.has(candidate.jobId))
        .slice(0, localQuota)
        .map((candidate) => rawConnections.find((conn) => conn.jobId === candidate.jobId))
        .filter(Boolean);

      nearestSelected.forEach((candidate) => {
        if (candidate.baseSize > 0) return;
        const donor = rawConnections
          .filter((conn) => conn.baseSize > POPULATION_GROUP_CONFIG.minimumFinalizeSize)
          .sort((a, b) => a.closeness - b.closeness)[0];
        if (donor) {
          donor.baseSize -= 1;
          candidate.baseSize += 1;
        }
      });

      let activeConnections = rawConnections.filter((conn) => conn.baseSize > 0);
      const minSize = POPULATION_GROUP_CONFIG.minimumFinalizeSize;

      while (activeConnections.length > 1) {
        const victim = activeConnections.find((conn) => conn.baseSize < minSize);
        if (!victim) break;

        const recipient = activeConnections
          .filter((conn) => conn !== victim)
          .sort((a, b) => {
            const distanceDeltaA = Math.abs(a.distanceMeters - victim.distanceMeters);
            const distanceDeltaB = Math.abs(b.distanceMeters - victim.distanceMeters);
            if (distanceDeltaA !== distanceDeltaB) return distanceDeltaA - distanceDeltaB;
            return b.closeness - a.closeness;
          })[0];

        if (!recipient) break;

        recipient.baseSize += victim.baseSize;
        victim.baseSize = 0;
        activeConnections = activeConnections.filter((conn) => conn.baseSize > 0);
      }
    }

    rawConnections.forEach((conn) => {
      if (conn.baseSize <= 0) return;
      const groupSizes = splitConnectionIntoGroups(conn.baseSize, conn.residenceId, conn.jobId);
      groupSizes.forEach((groupSize) => {
        const id = (connectionCounter++).toString();
        const residentNodeId = combinedIdMap[conn.residenceId];
        const jobNodeId = combinedIdMap[conn.jobId];
        if (!residentNodeId || !jobNodeId) return;
        if (combinedNeighborhoods[residentNodeId]) combinedNeighborhoods[residentNodeId].popIds.push(id);
        if (combinedNeighborhoods[jobNodeId]) combinedNeighborhoods[jobNodeId].popIds.push(id);
        neighborhoodConnections.push({
          residenceId: residentNodeId,
          jobId: jobNodeId,
          size: groupSize,
          drivingDistance: conn.drivingDistance,
          drivingSeconds: conn.drivingSeconds,
          id,
        });
      });
    });
  });

  const totalTerminalTrips = neighborhoodConnections
    .filter((conn) => {
      const originalId = combinedIdMapReverse[conn.jobId];
      return originalId && finalVoronoiMetadata[originalId]?.isTerminal;
    })
    .reduce((sum, conn) => sum + conn.size, 0);

  const globalTerminalCap = Math.round(totalPopulation * DISTANCE_WEIGHTING.globalTerminalShare);

  if (totalTerminalTrips > globalTerminalCap) {
    let over = totalTerminalTrips - globalTerminalCap;
    const terminalConnectionsDescending = neighborhoodConnections
      .map((conn, index) => ({ conn, index }))
      .filter(({ conn }) => {
        const originalId = combinedIdMapReverse[conn.jobId];
        return originalId && finalVoronoiMetadata[originalId]?.isTerminal;
      })
      .sort((a, b) => b.conn.size - a.conn.size);

    terminalConnectionsDescending.forEach(({ conn, index }) => {
      if (over <= 0) return;
      const reducible = Math.min(conn.size - 1, over);
      if (reducible <= 0) return;
      neighborhoodConnections[index].size -= reducible;
      over -= reducible;
    });
  }

  const terminalGroupMap = {};
  neighborhoodConnections.forEach((conn, index) => {
    const originalId = combinedIdMapReverse[conn.jobId];
    if (!originalId || !finalVoronoiMetadata[originalId]?.isTerminal) return;
    const meta = terminalMetaByDisplay[conn.jobId] || terminalMetaByPlace[originalId] || {};
    const groupKey = meta.iata || meta.displayName || originalId;
    if (!terminalGroupMap[groupKey]) terminalGroupMap[groupKey] = { indices: [], total: 0 };
    terminalGroupMap[groupKey].indices.push(index);
    terminalGroupMap[groupKey].total += neighborhoodConnections[index].size;
  });

  const updatedTerminalTotal = Object.values(terminalGroupMap).reduce((sum, group) => sum + group.total, 0);

  if (updatedTerminalTotal > 0) {
    Object.entries(terminalGroupMap).forEach(([key, group]) => {
      const desired = Math.round(globalTerminalCap * (group.total / updatedTerminalTotal));
      if (group.total > desired) {
        let over = group.total - desired;
        const sorted = group.indices
          .map((index) => ({ index, conn: neighborhoodConnections[index] }))
          .sort((a, b) => a.conn.closeness - b.conn.closeness);
        for (const item of sorted) {
          if (over <= 0) break;
          const reducible = Math.min(neighborhoodConnections[item.index].size - 1, over);
          if (reducible <= 0) continue;
          neighborhoodConnections[item.index].size -= reducible;
          over -= reducible;
        }
      }
    });
  }

  const finalPoints = Object.values(combinedNeighborhoods)
    .map((point) => {
      const originalId = combinedIdMapReverse[point.id];
      const metadata = originalId ? finalVoronoiMetadata[originalId] : null;
      if (metadata?.isTerminal) {
        return {
          ...point,
          residents: 0,
          tags: {
            ...point.tags,
            'subwaybuilder:is_terminal': 'true',
          },
        };
      }
      return point;
    })
    .filter((point) => point.jobs > 0 || point.residents > 0);

  return {
    points: finalPoints,
    pops: neighborhoodConnections.filter((conn) => conn.size > 0),
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
