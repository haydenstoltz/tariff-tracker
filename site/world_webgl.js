(function () {
  const DEG2RAD = Math.PI / 180;
  const DEFAULT_GRADE_COLORS = {
    A: "rgba(16, 185, 129, 0.56)",
    B: "rgba(34, 211, 238, 0.52)",
    C: "rgba(147, 197, 253, 0.48)",
    D: "rgba(245, 158, 11, 0.54)",
    F: "rgba(239, 68, 68, 0.52)",
    "N/A": "rgba(148, 163, 184, 0.42)"
  };

  function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
  }

  function normalizeLongitude(value) {
    let out = Number(value || 0);
    while (out > 180) out -= 360;
    while (out < -180) out += 360;
    return out;
  }

  function clampLatitude(value) {
    return clamp(Number(value || 0), -89.5, 89.5);
  }

  function lonLatToVector3(lon, lat, radius) {
    const safeLon = normalizeLongitude(lon);
    const safeLat = clampLatitude(lat);
    const phi = (90 - safeLat) * DEG2RAD;
    const theta = (safeLon + 180) * DEG2RAD;
    const x = -(radius * Math.sin(phi) * Math.cos(theta));
    const z = radius * Math.sin(phi) * Math.sin(theta);
    const y = radius * Math.cos(phi);
    return new THREE.Vector3(x, y, z);
  }

  function parseColorToHex(color) {
    try {
      const c = new THREE.Color(color || "#9fb7d8");
      return `#${c.getHexString()}`;
    } catch {
      return "#9fb7d8";
    }
  }

  function shadeHex(color, factor) {
    const base = new THREE.Color(parseColorToHex(color));
    const f = Number.isFinite(Number(factor)) ? Number(factor) : 0;
    const adjust = 1 + f;
    base.r = clamp(base.r * adjust, 0, 1);
    base.g = clamp(base.g * adjust, 0, 1);
    base.b = clamp(base.b * adjust, 0, 1);
    return `#${base.getHexString()}`;
  }

  function tinyNoise(lon, lat) {
    const a = Math.sin((lon + 13.17) * 0.14);
    const b = Math.cos((lat - 7.31) * 0.19);
    const c = Math.sin((lon + lat) * 0.11);
    const d = Math.cos((lon * 0.27) - (lat * 0.16));
    return (a + b + c + d) * 0.25;
  }

  function buildProceduralOceanTexture(size) {
    const width = Math.max(512, Number(size || 1024));
    const height = Math.round(width / 2);
    const canvas = document.createElement("canvas");
    canvas.width = width;
    canvas.height = height;
    const ctx = canvas.getContext("2d", { alpha: false, desynchronized: true });

    const gradient = ctx.createLinearGradient(0, 0, 0, height);
    gradient.addColorStop(0, "#3f9cde");
    gradient.addColorStop(0.44, "#1f73b6");
    gradient.addColorStop(1, "#082b52");
    ctx.fillStyle = gradient;
    ctx.fillRect(0, 0, width, height);

    const image = ctx.getImageData(0, 0, width, height);
    const pixels = image.data;

    for (let y = 0; y < height; y += 1) {
      const latNorm = 1 - Math.abs((y / (height - 1)) * 2 - 1);
      for (let x = 0; x < width; x += 1) {
        const i = (y * width + x) * 4;
        const lon = (x / width) * 360 - 180;
        const lat = 90 - (y / height) * 180;

        const n = tinyNoise(lon, lat) * 0.5 + tinyNoise(lon * 1.8, lat * 1.8) * 0.35;
        const glint = Math.max(0, Math.sin((x * 0.028) + (y * 0.015))) * 0.06;
        const bump = (n * 0.08) + (latNorm * 0.06) + glint;

        pixels[i] = clamp(pixels[i] + 255 * bump, 0, 255);
        pixels[i + 1] = clamp(pixels[i + 1] + 255 * bump * 0.78, 0, 255);
        pixels[i + 2] = clamp(pixels[i + 2] + 255 * bump * 0.44, 0, 255);
      }
    }

    ctx.putImageData(image, 0, 0);
    const texture = new THREE.CanvasTexture(canvas);
    texture.colorSpace = THREE.SRGBColorSpace;
    texture.wrapS = THREE.RepeatWrapping;
    texture.wrapT = THREE.ClampToEdgeWrapping;
    texture.anisotropy = 4;
    texture.needsUpdate = true;
    return texture;
  }

  function buildReliefTexture(size) {
    const width = Math.max(512, Number(size || 1024));
    const height = Math.round(width / 2);
    const canvas = document.createElement("canvas");
    canvas.width = width;
    canvas.height = height;
    const ctx = canvas.getContext("2d", { alpha: false, desynchronized: true });
    const image = ctx.createImageData(width, height);
    const pixels = image.data;

    for (let y = 0; y < height; y += 1) {
      for (let x = 0; x < width; x += 1) {
        const i = (y * width + x) * 4;
        const lon = (x / width) * 360 - 180;
        const lat = 90 - (y / height) * 180;

        const n0 = tinyNoise(lon * 0.95, lat * 0.95);
        const n1 = tinyNoise(lon * 1.9, lat * 1.9) * 0.55;
        const n2 = tinyNoise(lon * 3.8, lat * 3.8) * 0.25;
        const ridge = Math.abs(Math.sin((lon + lat) * 0.18)) * 0.2;
        const value = clamp((n0 * 0.55 + n1 + n2 + ridge + 1.1) * 0.45, 0, 1);
        const shade = Math.round(value * 255);

        pixels[i] = shade;
        pixels[i + 1] = shade;
        pixels[i + 2] = shade;
        pixels[i + 3] = 255;
      }
    }

    ctx.putImageData(image, 0, 0);
    const texture = new THREE.CanvasTexture(canvas);
    texture.colorSpace = THREE.NoColorSpace;
    texture.wrapS = THREE.RepeatWrapping;
    texture.wrapT = THREE.ClampToEdgeWrapping;
    texture.needsUpdate = true;
    return texture;
  }

  function textureSizeByWidth(width) {
    const w = Number(width || window.innerWidth || 1280);
    if (w >= 1800) return 2048;
    if (w >= 1200) return 1536;
    if (w >= 800) return 1024;
    return 768;
  }

  class WorldWebglRenderer {
    static supportsWebgl() {
      if (typeof window === "undefined" || !window.WebGLRenderingContext) return false;
      const canvas = document.createElement("canvas");
      const gl = canvas.getContext("webgl2") || canvas.getContext("webgl") || canvas.getContext("experimental-webgl");
      return Boolean(gl);
    }

    constructor(options = {}) {
      this.options = options;
      this.handlers = options.handlers || {};
      this.gradeColorMap = { ...DEFAULT_GRADE_COLORS, ...(options.gradeColorMap || {}) };
      this.container = options.container || null;
      this.radius = Number(options.radius || 100);
      this.routeActors = [];
      this.pointer = { clientX: 0, clientY: 0 };
      this.destroyed = false;
      this.animFrame = null;
      this.lastHoverMeta = null;
      this.landReliefInfluence = clamp(Number(options.landReliefInfluence || 0.12), 0, 0.18);

      this.scene = null;
      this.camera = null;
      this.renderer = null;
      this.controls = null;
      this.globe = null;
      this.routeGroup = null;
      this.shipGroup = null;
      this.truckGroup = null;
      this.oceanTexture = null;
      this.reliefTexture = null;
    }

    init() {
      if (!this.container) throw new Error("WebGL container is required.");
      if (typeof THREE === "undefined") throw new Error("THREE is unavailable.");
      if (typeof ThreeGlobe === "undefined") throw new Error("ThreeGlobe is unavailable.");
      if (!THREE.OrbitControls) throw new Error("OrbitControls is unavailable.");

      const rect = this.container.getBoundingClientRect();
      const width = Math.max(240, Math.round(rect.width || this.container.clientWidth || 960));
      const height = Math.max(220, Math.round(rect.height || this.container.clientHeight || 640));

      this.scene = new THREE.Scene();
      this.camera = new THREE.PerspectiveCamera(34, width / height, 0.1, 2600);
      this.camera.position.set(0, 0, 320);

      this.renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true, powerPreference: "high-performance" });
      this.renderer.setClearColor(0x000000, 0);
      this.renderer.outputColorSpace = THREE.SRGBColorSpace;
      this.renderer.toneMapping = THREE.ACESFilmicToneMapping;
      this.renderer.toneMappingExposure = 1.03;
      this.renderer.setPixelRatio(clamp(window.devicePixelRatio || 1, 1, Number(this.options.maxDpr || 1.5)));
      this.renderer.setSize(width, height, false);
      this.renderer.domElement.className = "world-webgl-canvas";
      this.renderer.domElement.setAttribute("aria-hidden", "true");
      this.container.appendChild(this.renderer.domElement);

      this.controls = new THREE.OrbitControls(this.camera, this.renderer.domElement);
      this.controls.enablePan = false;
      this.controls.enableDamping = true;
      this.controls.dampingFactor = 0.08;
      this.controls.rotateSpeed = 0.45;
      this.controls.zoomSpeed = 0.75;
      this.controls.minDistance = 205;
      this.controls.maxDistance = 480;
      this.controls.autoRotate = true;
      this.controls.autoRotateSpeed = 0.42;
      this.controls.target.set(0, 0, 0);

      this.controls.addEventListener("start", () => {
        this.controls.autoRotate = false;
        if (typeof this.handlers.onManualOrbitStart === "function") {
          this.handlers.onManualOrbitStart();
        }
      });
      this.controls.addEventListener("change", () => {
        if (typeof this.handlers.onCameraChange === "function") {
          this.handlers.onCameraChange(this.cameraLonLat());
        }
      });

      this.renderer.domElement.addEventListener("pointermove", event => {
        this.pointer = { clientX: event.clientX, clientY: event.clientY };
      });

      const keyLight = new THREE.DirectionalLight(0xbfe3ff, 1.1);
      keyLight.position.set(190, 120, 160);
      this.scene.add(keyLight);

      const fillLight = new THREE.DirectionalLight(0x2f7eb8, 0.44);
      fillLight.position.set(-210, -130, -90);
      this.scene.add(fillLight);

      const ambient = new THREE.AmbientLight(0x7bb7ea, 0.46);
      this.scene.add(ambient);

      this.globe = new ThreeGlobe({ waitForGlobeReady: false, animateIn: false })
        .showAtmosphere(true)
        .atmosphereColor("#9fdcff")
        .atmosphereAltitude(0.12)
        .polygonsTransitionDuration(120)
        .polygonAltitude(d => Number(d?.__meta?.altitude || 0.0012))
        .polygonCapColor(d => String(d?.__meta?.capColor || "#8fb4dd"))
        .polygonSideColor(d => String(d?.__meta?.sideColor || "rgba(26, 52, 82, 0.46)"))
        .polygonStrokeColor(d => String(d?.__meta?.strokeColor || "rgba(214, 230, 246, 0.65)"));

      this.globe.onPolygonHover((polygon) => {
        const meta = polygon?.__meta || null;
        if (!meta) {
          if (this.lastHoverMeta && typeof this.handlers.onCountryHover === "function") {
            this.handlers.onCountryHover(null, this.pointer);
          }
          this.lastHoverMeta = null;
          return;
        }

        if (this.lastHoverMeta && this.lastHoverMeta.atlasName === meta.atlasName) {
          if (typeof this.handlers.onCountryHover === "function") {
            this.handlers.onCountryHover(meta, this.pointer);
          }
          return;
        }

        this.lastHoverMeta = meta;
        if (typeof this.handlers.onCountryHover === "function") {
          this.handlers.onCountryHover(meta, this.pointer);
        }
      });

      this.globe.onPolygonClick((polygon) => {
        const meta = polygon?.__meta || null;
        if (!meta || typeof this.handlers.onCountryClick !== "function") return;
        this.handlers.onCountryClick(meta, this.pointer);
      });

      this.scene.add(this.globe);

      this.routeGroup = new THREE.Group();
      this.shipGroup = new THREE.Group();
      this.truckGroup = new THREE.Group();
      this.scene.add(this.routeGroup);
      this.scene.add(this.shipGroup);
      this.scene.add(this.truckGroup);

      const texSize = textureSizeByWidth(width);
      this.oceanTexture = buildProceduralOceanTexture(texSize);
      this.reliefTexture = buildReliefTexture(texSize);

      const globeMaterial = this.globe.globeMaterial();
      globeMaterial.color = new THREE.Color("#123f71");
      globeMaterial.map = this.oceanTexture;
      globeMaterial.bumpMap = this.reliefTexture;
      globeMaterial.bumpScale = 1.4;
      globeMaterial.specular = new THREE.Color("#56b0ea");
      globeMaterial.shininess = 34;
      globeMaterial.needsUpdate = true;

      this.startLoop();
    }

    startLoop() {
      const loop = (elapsed) => {
        if (this.destroyed) return;
        if (this.controls) this.controls.update();
        this.updateVehicles(elapsed || 0);
        if (this.renderer && this.scene && this.camera) {
          this.renderer.render(this.scene, this.camera);
        }
        this.animFrame = window.requestAnimationFrame(loop);
      };
      this.animFrame = window.requestAnimationFrame(loop);
    }

    stopLoop() {
      if (this.animFrame) {
        window.cancelAnimationFrame(this.animFrame);
        this.animFrame = null;
      }
    }

    clearGroup(group) {
      if (!group) return;
      while (group.children.length) {
        const child = group.children.pop();
        if (child.geometry) child.geometry.dispose();
        if (child.material) {
          if (Array.isArray(child.material)) child.material.forEach(m => m.dispose());
          else child.material.dispose();
        }
      }
    }

    cameraLonLat() {
      if (!this.camera) return { lon: 0, lat: 0 };
      const normal = this.camera.position.clone().normalize();
      const lat = THREE.MathUtils.radToDeg(Math.asin(clamp(normal.y, -1, 1)));
      const lon = THREE.MathUtils.radToDeg(Math.atan2(normal.z, normal.x));
      return {
        lon: normalizeLongitude(lon),
        lat: clampLatitude(lat)
      };
    }

    patchLandReliefMaterial(material) {
      if (!material || !this.reliefTexture) return;
      if (material.userData?.worldLandReliefPatched) return;

      const reliefTexture = this.reliefTexture;
      const reliefInfluence = this.landReliefInfluence;
      const priorHook = material.onBeforeCompile;

      material.onBeforeCompile = shader => {
        if (typeof priorHook === "function") {
          priorHook(shader);
        }

        shader.uniforms.worldReliefMap = { value: reliefTexture };
        shader.uniforms.worldReliefInfluence = { value: reliefInfluence };

        shader.vertexShader = shader.vertexShader
          .replace(
            "#include <common>",
            "#include <common>\nvarying vec3 worldLandReliefWorldPos;"
          )
          .replace(
            "#include <worldpos_vertex>",
            "#include <worldpos_vertex>\nworldLandReliefWorldPos = worldPosition.xyz;"
          );

        shader.fragmentShader = shader.fragmentShader
          .replace(
            "#include <common>",
            "#include <common>\nuniform sampler2D worldReliefMap;\nuniform float worldReliefInfluence;\nvarying vec3 worldLandReliefWorldPos;"
          )
          .replace(
            "#include <color_fragment>",
            [
              "#include <color_fragment>",
              "vec3 worldReliefNormal = normalize(worldLandReliefWorldPos);",
              "float worldReliefU = atan(worldReliefNormal.z, worldReliefNormal.x) / (2.0 * PI) + 0.5;",
              "float worldReliefV = asin(clamp(worldReliefNormal.y, -1.0, 1.0)) / PI + 0.5;",
              "float worldReliefSample = texture2D(worldReliefMap, vec2(fract(worldReliefU), clamp(worldReliefV, 0.002, 0.998))).r;",
              "float worldReliefShift = (worldReliefSample - 0.5) * 2.0 * worldReliefInfluence;",
              "diffuseColor.rgb = clamp(diffuseColor.rgb * (1.0 + worldReliefShift), 0.0, 1.0);"
            ].join("\n")
          );
      };

      material.customProgramCacheKey = () => "world_land_relief_v1";
      material.userData = material.userData || {};
      material.userData.worldLandReliefPatched = true;
      material.needsUpdate = true;
    }

    applyLandReliefShaderToPolygons() {
      if (!this.globe || !this.reliefTexture) return;

      this.globe.traverse(node => {
        if (!node || node.__globeObjType !== "polygon") return;
        const mesh = Array.isArray(node.children)
          ? node.children.find(child => child?.isMesh)
          : null;
        if (!mesh || !mesh.material) return;

        const materials = Array.isArray(mesh.material) ? mesh.material : [mesh.material];
        const capMaterial = materials[1] || materials[0] || null;
        this.patchLandReliefMaterial(capMaterial);
      });
    }

    setCountries(countries) {
      const data = Array.isArray(countries) ? countries : [];
      data.forEach(feature => {
        if (!feature || !feature.__meta) return;
        const centroid = Array.isArray(feature.__meta.centroid) ? feature.__meta.centroid : [0, 0];
        const relief = tinyNoise(centroid[0], centroid[1]);
        const reliefFactor = clamp(relief * 0.12, -0.12, 0.12);
        feature.__meta.capColor = shadeHex(feature.__meta.baseColor, reliefFactor);
      });
      this.globe.polygonsData(data);
      this.applyLandReliefShaderToPolygons();
    }

    setAutoRotate(enabled) {
      if (!this.controls) return;
      this.controls.autoRotate = Boolean(enabled);
    }

    focusOn(lon, lat, distance = null) {
      if (!this.camera || !this.controls) return;
      const currentDistance = this.camera.position.length();
      const nextDistance = Number.isFinite(Number(distance)) ? Number(distance) : currentDistance;
      const pos = lonLatToVector3(lon, lat, clamp(nextDistance, this.controls.minDistance, this.controls.maxDistance));
      this.camera.position.copy(pos);
      this.camera.lookAt(0, 0, 0);
      this.controls.target.set(0, 0, 0);
      this.controls.update();
    }

    routeColorForGrade(grade) {
      const key = String(grade || "N/A").toUpperCase();
      return this.gradeColorMap[key] || this.gradeColorMap["N/A"];
    }

    setOverlay(overlay) {
      this.clearGroup(this.routeGroup);
      this.clearGroup(this.shipGroup);
      this.clearGroup(this.truckGroup);
      this.routeActors = [];

      const routes = Array.isArray(overlay?.routes) ? overlay.routes : [];
      const ships = Array.isArray(overlay?.ships) ? overlay.ships : [];
      const trucks = Array.isArray(overlay?.trucks) ? overlay.trucks : [];

      routes.forEach(route => {
        const coords = Array.isArray(route?.routeGeo?.coordinates) ? route.routeGeo.coordinates : [];
        if (coords.length < 2) return;

        const altitude = route.mode === "land" ? 1.15 : 1.45;
        const points = coords
          .map(p => Array.isArray(p) ? lonLatToVector3(p[0], p[1], this.radius + altitude) : null)
          .filter(Boolean);
        if (points.length < 2) return;

        const geometry = new THREE.BufferGeometry().setFromPoints(points);
        const color = parseColorToHex(this.routeColorForGrade(route.mutual_grade));
        const isLand = route.mode === "land";
        const material = isLand
          ? new THREE.LineDashedMaterial({ color, dashSize: 1.4, gapSize: 1.8, transparent: true, opacity: 0.84 })
          : new THREE.LineBasicMaterial({ color, transparent: true, opacity: 0.74 });

        const line = new THREE.Line(geometry, material);
        if (isLand && typeof line.computeLineDistances === "function") {
          line.computeLineDistances();
        }
        this.routeGroup.add(line);
      });

      const shipGeom = new THREE.SphereGeometry(0.9, 10, 10);
      const truckGeom = new THREE.BoxGeometry(1.4, 0.8, 0.9);

      ships.forEach(item => {
        if (typeof item?.routeAt !== "function") return;
        const mesh = new THREE.Mesh(
          shipGeom,
          new THREE.MeshStandardMaterial({ color: 0xe8f7ff, emissive: 0x2b7db3, emissiveIntensity: 0.26, roughness: 0.45, metalness: 0.18 })
        );
        this.shipGroup.add(mesh);
        this.routeActors.push({ kind: "ship", mesh, routeAt: item.routeAt, speed: Number(item.speed || 0), offset: Number(item.offset || 0) });
      });

      trucks.forEach(item => {
        if (typeof item?.routeAt !== "function") return;
        const mesh = new THREE.Mesh(
          truckGeom,
          new THREE.MeshStandardMaterial({ color: 0xc08d4c, emissive: 0x3f2f1d, emissiveIntensity: 0.16, roughness: 0.58, metalness: 0.08 })
        );
        this.truckGroup.add(mesh);
        this.routeActors.push({ kind: "truck", mesh, routeAt: item.routeAt, speed: Number(item.speed || 0), offset: Number(item.offset || 0) });
      });
    }

    updateVehicles(elapsed) {
      if (!this.camera || !this.routeActors.length) return;
      const cameraNorm = this.camera.position.clone().normalize();

      this.routeActors.forEach(actor => {
        const t = (elapsed * actor.speed + actor.offset) % 1;
        const point = actor.routeAt(t);
        if (!Array.isArray(point) || point.length < 2) {
          actor.mesh.visible = false;
          return;
        }

        const altitude = actor.kind === "truck" ? 1.18 : 1.58;
        const pos = lonLatToVector3(point[0], point[1], this.radius + altitude);
        const normal = pos.clone().normalize();
        const visible = normal.dot(cameraNorm) > 0;
        actor.mesh.visible = visible;
        if (!visible) return;

        actor.mesh.position.copy(pos);

        const nextPoint = actor.routeAt((t + 0.0025) % 1);
        if (Array.isArray(nextPoint) && nextPoint.length >= 2) {
          const target = lonLatToVector3(nextPoint[0], nextPoint[1], this.radius + altitude);
          actor.mesh.lookAt(target);
        } else {
          actor.mesh.lookAt(pos.clone().multiplyScalar(1.02));
        }
      });
    }

    resize() {
      if (!this.renderer || !this.camera || !this.container) return;
      const rect = this.container.getBoundingClientRect();
      const width = Math.max(240, Math.round(rect.width || this.container.clientWidth || 960));
      const height = Math.max(220, Math.round(rect.height || this.container.clientHeight || 640));
      this.camera.aspect = width / height;
      this.camera.updateProjectionMatrix();
      this.renderer.setPixelRatio(clamp(window.devicePixelRatio || 1, 1, Number(this.options.maxDpr || 1.5)));
      this.renderer.setSize(width, height, false);
    }

    destroy() {
      this.destroyed = true;
      this.stopLoop();
      this.clearGroup(this.routeGroup);
      this.clearGroup(this.shipGroup);
      this.clearGroup(this.truckGroup);
      if (this.globe) {
        try {
          this.scene.remove(this.globe);
        } catch {
        }
      }
      if (this.oceanTexture) this.oceanTexture.dispose();
      if (this.reliefTexture) this.reliefTexture.dispose();
      if (this.renderer) {
        this.renderer.dispose();
        this.renderer.domElement.remove();
      }
      if (this.controls) {
        this.controls.dispose();
      }
      this.scene = null;
      this.camera = null;
      this.controls = null;
      this.renderer = null;
      this.globe = null;
    }
  }

  window.WorldWebglRenderer = WorldWebglRenderer;
})();
