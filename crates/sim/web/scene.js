// 3D "digital twin" of the three data centers. Procedural geometry only; no model files.
// The page feeds it the Concord cluster JSON; the scene never decides anything about Raft.

import * as THREE from 'three';
import { OrbitControls } from '/vendor/OrbitControls.js';

const HALLS = {
  1: { name: 'Ashburn', pos: new THREE.Vector3(-7, 0, 4.5) },
  2: { name: 'Frankfurt', pos: new THREE.Vector3(7, 0, 4.5) },
  3: { name: 'Singapore', pos: new THREE.Vector3(0, 0, -7.5) },
};
const PAIRS = [[1, 2], [2, 3], [1, 3]];
const BAYS_X = 6, BAYS_Y = 8, BAYS = BAYS_X * BAYS_Y;
const RACKS_PER_ROW = 7;
const MAX_PULSES = 96;

const PALETTES = {
  // Stone & Ink. Flat, neutral materials: no glow, no gradients.
  light: {
    bg: 0xe7e4de, fog: 0xe7e4de, ground: 0xe2ded6, grid: 0xd5d0c6, gridMain: 0xc4beb2,
    slab: 0xcfcac0, rack: 0x2e2c29, rackEdge: 0x4a4741, shell: 0x4a4741, glass: 0x8a857c,
    aisle: 0xd8d3c9, led: 0xf2efe8, ledAlt: 0xd9a35a, link: 0x6b675f, cut: 0xb3261e,
    bayEmpty: 0x45423d, bayDisk: 0xf2efe8, bayMem: 0xd9a35a, beacon: 0x1c1b19, elect: 0x8a5a12,
    alarm: 0xb3261e, label: '#1c1b19', labelSub: '#5e5a53', baseline: 0x8a5a12, concord: 0x2e6b4f,
    hemiSky: 0xffffff, hemiGround: 0xb8b3aa, sun: 0xffffff,
    pulse: { AppendEntries: [0.11, 0.11, 0.1], Heartbeat: [0.6, 0.58, 0.54], RequestVote: [0.54, 0.35, 0.07], VoteReply: [0.62, 0.45, 0.2], AppendReply: [0.4, 0.38, 0.35] },
  },
  dark: {
    bg: 0x1b1a18, fog: 0x1b1a18, ground: 0x201f1c, grid: 0x2c2a27, gridMain: 0x3a3834,
    slab: 0x2c2a27, rack: 0x151412, rackEdge: 0x4a4741, shell: 0xa8a39a, glass: 0xece9e3,
    aisle: 0x2c2a27, led: 0xece9e3, ledAlt: 0xd9a35a, link: 0x8f8a80, cut: 0xf0705f,
    bayEmpty: 0x34322e, bayDisk: 0xece9e3, bayMem: 0xd9a35a, beacon: 0xece9e3, elect: 0xd9a35a,
    alarm: 0xf0705f, label: '#ece9e3', labelSub: '#a8a39a', baseline: 0xd9a35a, concord: 0x7fb79a,
    hemiSky: 0xfff6ea, hemiGround: 0x141311, sun: 0xfff1e0,
    pulse: { AppendEntries: [0.93, 0.91, 0.89], Heartbeat: [0.42, 0.4, 0.37], RequestVote: [0.85, 0.64, 0.35], VoteReply: [0.75, 0.56, 0.3], AppendReply: [0.65, 0.63, 0.6] },
  },
};

function hasWebGL() {
  try {
    const c = document.createElement('canvas');
    return !!(window.WebGLRenderingContext && (c.getContext('webgl2') || c.getContext('webgl')));
  } catch (e) {
    return false;
  }
}

function discTexture() {
  const c = document.createElement('canvas');
  c.width = c.height = 64;
  const g = c.getContext('2d');
  g.fillStyle = '#ffffff';
  g.beginPath();
  g.arc(32, 32, 26, 0, Math.PI * 2);
  g.fill();
  return new THREE.CanvasTexture(c);
}

/** A sprite whose text can be redrawn. Two lines: title and subtitle. */
function makeLabel() {
  const canvas = document.createElement('canvas');
  canvas.width = 768;
  canvas.height = 170;
  const tex = new THREE.CanvasTexture(canvas);
  tex.colorSpace = THREE.SRGBColorSpace;
  const mat = new THREE.SpriteMaterial({ map: tex, transparent: true, depthWrite: false });
  const sprite = new THREE.Sprite(mat);
  sprite.scale.set(9.6, 2.125, 1);
  let last = '';
  sprite.userData.reset = () => { last = ''; };
  sprite.userData.draw = (title, sub, colors, accent) => {
    const key = title + '|' + sub + '|' + colors.label + '|' + (accent || '');
    if (key === last) return;
    last = key;
    const g = canvas.getContext('2d');
    g.clearRect(0, 0, canvas.width, canvas.height);
    g.textAlign = 'center';
    g.fillStyle = colors.label;
    // Shrink a line until it fits the label, so long states are never clipped.
    const fit = (text, weight, size, y) => {
      let px = size;
      do { g.font = `${weight} ${px}px "Public Sans", system-ui, sans-serif`; px -= 2; } while (g.measureText(text).width > 740 && px > 20);
      g.fillText(text, 384, y);
    };
    fit(title.toUpperCase(), 700, 60, 66);
    g.fillStyle = accent || colors.labelSub;
    fit(sub, 600, 48, 138);
    tex.needsUpdate = true;
  };
  return sprite;
}

export function createScene(container, { reducedMotion = false, onSelect = () => {} } = {}) {
  if (!hasWebGL()) throw new Error('webgl unavailable');

  const small = Math.min(container.clientWidth, container.clientHeight) < 520;
  let theme = matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  let P = PALETTES[theme];

  const renderer = new THREE.WebGLRenderer({ antialias: !small, alpha: false, powerPreference: 'high-performance' });
  let pixelRatio = Math.min(window.devicePixelRatio || 1, small ? 1.5 : 2);
  renderer.setPixelRatio(pixelRatio);
  renderer.toneMapping = THREE.NoToneMapping;
  container.appendChild(renderer.domElement);
  renderer.domElement.setAttribute('aria-hidden', 'true');

  const scene = new THREE.Scene();
  const camera = new THREE.PerspectiveCamera(38, 1, 0.5, 400);
  const controls = new OrbitControls(camera, renderer.domElement);
  controls.enableDamping = !reducedMotion;
  controls.dampingFactor = 0.08;
  controls.maxPolarAngle = Math.PI * 0.46;
  controls.minDistance = 9;
  controls.maxDistance = 80;
  controls.target.set(0, 0, 0);

  const hemi = new THREE.HemisphereLight(P.hemiSky, P.hemiGround, 0.9);
  const sun = new THREE.DirectionalLight(P.sun, 1.2);
  sun.position.set(-20, 40, 25);
  scene.add(hemi, sun);

  // ---------- ground ----------
  const ground = new THREE.Mesh(
    new THREE.CircleGeometry(260, 64),
    new THREE.MeshStandardMaterial({ color: P.ground, roughness: 0.95, metalness: 0.05 }),
  );
  ground.rotation.x = -Math.PI / 2;
  ground.position.y = -0.02;
  let grid = null;
  function makeGrid() {
    if (grid) {
      scene.remove(grid);
      grid.geometry.dispose();
      grid.material.dispose();
    }
    grid = new THREE.GridHelper(240, 120, P.gridMain, P.grid);
    grid.material.transparent = true;
    grid.material.opacity = 0.55;
    scene.add(grid);
  }
  scene.add(ground);

  const mats = {
    slab: new THREE.MeshStandardMaterial({ color: P.slab, roughness: 0.6, metalness: 0.4 }),
    rack: new THREE.MeshStandardMaterial({ color: P.rack, roughness: 0.45, metalness: 0.6 }),
    glass: new THREE.MeshStandardMaterial({ color: P.glass, transparent: true, opacity: 0.06, depthWrite: false, side: THREE.DoubleSide }),
    shell: new THREE.LineBasicMaterial({ color: P.shell, transparent: true, opacity: 0.9 }),
    aisle: new THREE.MeshBasicMaterial({ color: P.aisle, transparent: true, opacity: 0.8 }),
    led: new THREE.MeshBasicMaterial({ color: 0xffffff }),
    bay: new THREE.MeshBasicMaterial({ color: 0xffffff }),
    storage: new THREE.MeshStandardMaterial({ color: P.rack, roughness: 0.4, metalness: 0.7 }),
    shellDown: new THREE.LineBasicMaterial({ color: P.alarm, transparent: true, opacity: 1 }),
  };

  // ---------- halls ----------
  const halls = {};
  const pickables = [];
  const tmp = new THREE.Object3D();
  const color = new THREE.Color();

  for (const [idStr, def] of Object.entries(HALLS)) {
    const id = Number(idStr);
    const g = new THREE.Group();
    g.position.copy(def.pos);
    // Face the campus center so the storage rack is visible from the default view.
    g.lookAt(0, 0, 0);
    scene.add(g);

    const slab = new THREE.Mesh(new THREE.BoxGeometry(9, 0.3, 6.4), mats.slab);
    slab.position.y = 0.15;
    slab.userData.hall = id;
    g.add(slab);
    pickables.push(slab);

    const shellGeo = new THREE.BoxGeometry(9, 3.4, 6.4);
    const walls = new THREE.Mesh(shellGeo, mats.glass);
    walls.position.y = 2.0;
    walls.userData.hall = id;
    pickables.push(walls);
    const edges = new THREE.LineSegments(new THREE.EdgesGeometry(shellGeo), mats.shell);
    edges.position.y = 2.0;
    g.add(walls, edges);

    // Cold aisle between the two rack rows.
    const aisle = new THREE.Mesh(new THREE.PlaneGeometry(6.2, 1.1), mats.aisle);
    aisle.rotation.x = -Math.PI / 2;
    aisle.position.set(-0.6, 0.32, 0);
    g.add(aisle);

    const racks = new THREE.InstancedMesh(new THREE.BoxGeometry(0.72, 2.3, 1.0), mats.rack, RACKS_PER_ROW * 2);
    const leds = new THREE.InstancedMesh(new THREE.BoxGeometry(0.5, 0.06, 0.02), mats.led, RACKS_PER_ROW * 2 * 6);
    let r = 0, l = 0;
    for (const row of [-1, 1]) {
      for (let i = 0; i < RACKS_PER_ROW; i++) {
        const x = -3.6 + i * 0.86;
        const z = row * 1.15;
        tmp.position.set(x, 1.45, z);
        tmp.rotation.set(0, 0, 0);
        tmp.updateMatrix();
        racks.setMatrixAt(r++, tmp.matrix);
        for (let k = 0; k < 6; k++) {
          // LEDs on the face that looks into the cold aisle.
          tmp.position.set(x, 0.75 + k * 0.32, z - row * 0.51);
          tmp.updateMatrix();
          leds.setMatrixAt(l, tmp.matrix);
          leds.setColorAt(l, color.set(P.led));
          l++;
        }
      }
    }
    g.add(racks, leds);

    // Storage rack against the wall that faces the campus center (local +z), so its drive bays
    // face the default camera. One bay per order saved on this hall's disk.
    const storage = new THREE.Mesh(new THREE.BoxGeometry(2.6, 2.75, 0.8), mats.storage);
    storage.position.set(0.9, 1.68, 2.55);
    g.add(storage);
    const bays = new THREE.InstancedMesh(new THREE.BoxGeometry(0.3, 0.24, 0.03), mats.bay, BAYS);
    for (let y = 0; y < BAYS_Y; y++) {
      for (let x = 0; x < BAYS_X; x++) {
        const i = y * BAYS_X + x;
        tmp.position.set(x * 0.36, 0.62 + y * 0.29, 2.97);
        tmp.updateMatrix();
        bays.setMatrixAt(i, tmp.matrix);
        bays.setColorAt(i, color.set(P.bayEmpty));
      }
    }
    g.add(bays);

    // A plain ring above the main hall (and the hall holding an election).
    const ring = new THREE.Mesh(new THREE.TorusGeometry(1.1, 0.07, 10, 48), new THREE.MeshBasicMaterial({ color: P.beacon }));
    ring.rotation.x = Math.PI / 2;
    ring.position.y = 6.2;
    ring.visible = false;
    g.add(ring);

    const label = makeLabel();
    label.position.set(def.pos.x, 8.8, def.pos.z);
    scene.add(label);

    halls[id] = { id, def, leds, bays, ring, label, edges, state: null };
  }

  // ---------- fiber links ----------
  const links = {};
  for (const [a, b] of PAIRS) {
    const pa = HALLS[a].pos.clone().setY(3.6);
    const pb = HALLS[b].pos.clone().setY(3.6);
    const mid = pa.clone().add(pb).multiplyScalar(0.5).setY(9);
    const curve = new THREE.QuadraticBezierCurve3(pa, mid, pb);
    const mat = new THREE.MeshBasicMaterial({ color: P.link, transparent: true, opacity: 0.85, depthWrite: false });
    const whole = new THREE.Mesh(new THREE.TubeGeometry(curve, 64, 0.05, 8), mat);
    const half = (t0, t1) => new THREE.CatmullRomCurve3(Array.from({ length: 24 }, (_, i) => curve.getPoint(t0 + (t1 - t0) * i / 23)));
    const cutMat = new THREE.MeshBasicMaterial({ color: P.cut, transparent: true, opacity: 0.9 });
    const left = new THREE.Mesh(new THREE.TubeGeometry(half(0, 0.43), 32, 0.08, 8), cutMat);
    const right = new THREE.Mesh(new THREE.TubeGeometry(half(0.57, 1), 32, 0.08, 8), cutMat);
    const spark = new THREE.Sprite(new THREE.SpriteMaterial({ map: discTexture(), color: P.cut, transparent: true, depthWrite: false }));
    spark.position.copy(curve.getPoint(0.5));
    spark.scale.set(0.7, 0.7, 1);
    const cutGroup = new THREE.Group();
    cutGroup.add(left, right, spark);
    cutGroup.visible = false;
    scene.add(whole, cutGroup);
    links[`${a}-${b}`] = { a, b, curve, whole, mat, cutMat, cutGroup, spark, cut: false };
  }
  const linkFor = (x, y) => links[`${Math.min(x, y)}-${Math.max(x, y)}`];

  // ---------- message pulses ----------
  const pulseGeo = new THREE.BufferGeometry();
  const pulsePos = new Float32Array(MAX_PULSES * 3);
  const pulseCol = new Float32Array(MAX_PULSES * 3);
  pulseGeo.setAttribute('position', new THREE.BufferAttribute(pulsePos, 3));
  pulseGeo.setAttribute('color', new THREE.BufferAttribute(pulseCol, 3));
  const pulses = new THREE.Points(pulseGeo, new THREE.PointsMaterial({
    size: 0.6, map: discTexture(), vertexColors: true, transparent: true, alphaTest: 0.5, depthWrite: false, sizeAttenuation: true,
  }));
  pulses.frustumCulled = false;
  scene.add(pulses);

  // ---------- confirmation ripples ----------
  const ripples = [];
  function ripple(at, hex) {
    const m = new THREE.Mesh(
      new THREE.RingGeometry(0.6, 0.9, 64),
      new THREE.MeshBasicMaterial({ color: hex, transparent: true, opacity: 0.9, side: THREE.DoubleSide, depthWrite: false }),
    );
    m.rotation.x = -Math.PI / 2;
    m.position.set(at.x, 0.05, at.z);
    scene.add(m);
    ripples.push({ m, born: performance.now() });
  }

  // ---------- state ----------
  let state = null, stateAt = 0, tickMs = 90;

  function applyTheme(name) {
    theme = name;
    P = PALETTES[name];
    scene.background = new THREE.Color(P.bg);
    scene.fog = new THREE.Fog(P.fog, 60, 125);
    ground.material.color.set(P.ground);
    makeGrid();
    hemi.color.set(P.hemiSky);
    hemi.groundColor.set(P.hemiGround);
    sun.color.set(P.sun);
    mats.slab.color.set(P.slab);
    mats.rack.color.set(P.rack);
    mats.storage.color.set(P.rack);
    mats.glass.color.set(P.glass);
    mats.shell.color.set(P.shell);
    mats.aisle.color.set(P.aisle);
    mats.shellDown.color.set(P.alarm);
    for (const L of Object.values(links)) {
      L.mat.color.set(P.link);
      L.cutMat.color.set(P.cut);
      L.spark.material.color.set(P.cut);
    }
    for (const h of Object.values(halls)) h.ring.material.color.set(P.beacon);
    if (state) paintHalls();
  }

  function nodeById(id) {
    return state && state.nodes.find(n => n.id === id);
  }

  function linkDown(a, b) {
    if (!state) return false;
    const na = nodeById(a), nb = nodeById(b);
    return (state.cuts || []).some(([x, y]) => (x === Math.min(a, b) && y === Math.max(a, b)))
      || !!(na && na.isolated) || !!(nb && nb.isolated);
  }

  const ROLE_WORD = { leader: 'MAIN', follower: 'BACKUP', candidate: 'CHOOSING A MAIN', down: 'OFF' };

  function paintHalls() {
    for (const h of Object.values(halls)) {
      const n = nodeById(h.id);
      if (!n) continue;
      const alive = n.alive;
      const role = alive ? n.role : 'down';
      h.state = { alive, role, disk: n.disk_puts, mem: n.mem_puts };
      h.ring.visible = alive && (role === 'leader' || role === 'candidate');
      h.ring.material.color.set(role === 'candidate' ? P.elect : P.beacon);
      h.edges.material = alive ? mats.shell : mats.shellDown;
      // Drive bays: solid for orders on disk, bright for orders only in memory.
      const disk = Math.min(n.disk_puts, BAYS);
      const mem = Math.min(n.mem_puts, BAYS - disk);
      for (let i = 0; i < BAYS; i++) {
        let hex = P.bayEmpty;
        if (i < disk) hex = P.bayDisk;
        else if (i < disk + mem) hex = P.bayMem;
        if (!alive && i >= disk) hex = P.bayEmpty;
        h.bays.setColorAt(i, color.set(hex));
      }
      h.bays.instanceColor.needsUpdate = true;
      const accent = role === 'leader' ? '#' + new THREE.Color(P.beacon).getHexString()
        : role === 'down' ? '#' + new THREE.Color(P.alarm).getHexString() : null;
      const word = n.isolated && alive ? 'CUT OFF' : ROLE_WORD[role] || role.toUpperCase();
      const sub = `${word} · ${n.disk_puts} order${n.disk_puts === 1 ? '' : 's'}`;
      h.label.userData.draw(h.def.name, sub, P, accent);
    }
    for (const L of Object.values(links)) {
      L.cut = linkDown(L.a, L.b);
      L.whole.visible = !L.cut;
      L.cutGroup.visible = L.cut;
    }
  }

  function update(next, meta = {}) {
    state = next;
    stateAt = performance.now();
    if (meta.tickMs) tickMs = meta.tickMs;
    paintHalls();
    if (reducedMotion) drawPulses(1);
  }

  function drawPulses(fracOverride) {
    let n = 0;
    if (state) {
      const frac = fracOverride ?? Math.min(1, (performance.now() - stateAt) / Math.max(40, tickMs));
      for (const m of state.flight) {
        if (n >= MAX_PULSES) break;
        const L = linkFor(m.from, m.to);
        if (!L) continue;
        let t = Math.min(1, m.age + frac);
        if (L.cut) t = Math.min(t, 0.4);
        const forward = m.from === L.a;
        const p = L.curve.getPoint(forward ? t : 1 - t);
        pulsePos[n * 3] = p.x; pulsePos[n * 3 + 1] = p.y; pulsePos[n * 3 + 2] = p.z;
        const c = P.pulse[m.kind] || P.pulse.AppendEntries;
        pulseCol[n * 3] = c[0]; pulseCol[n * 3 + 1] = c[1]; pulseCol[n * 3 + 2] = c[2];
        n++;
      }
    }
    pulseGeo.setDrawRange(0, n);
    pulseGeo.attributes.position.needsUpdate = true;
    pulseGeo.attributes.color.needsUpdate = true;
  }

  // ---------- camera ----------
  const PRESETS = {
    overview: { pos: new THREE.Vector3(0, 36, 42), target: new THREE.Vector3(0, 0, -2.5) },
  };
  for (const [id, def] of Object.entries(HALLS)) {
    // Racks face the campus center, so look from the center side, high and slightly off-axis.
    const inward = def.pos.clone().setY(0).normalize().multiplyScalar(-1);
    const side = new THREE.Vector3(-inward.z, 0, inward.x).multiplyScalar(2.5);
    PRESETS['hall' + id] = {
      pos: def.pos.clone().add(inward.clone().multiplyScalar(15.5)).add(side).add(new THREE.Vector3(0, 11, 0)),
      target: def.pos.clone().add(inward.clone().multiplyScalar(1.5)).setY(1.4),
    };
  }
  let flight = null;
  function fly(name) {
    const p = PRESETS[name];
    if (!p) return;
    if (reducedMotion) {
      camera.position.copy(p.pos);
      controls.target.copy(p.target);
      controls.update();
      return;
    }
    flight = { from: camera.position.clone(), fromT: controls.target.clone(), to: p.pos, toT: p.target, t0: performance.now(), dur: 1700 };
  }
  camera.position.copy(PRESETS.overview.pos);
  controls.target.copy(PRESETS.overview.target);

  // ---------- picking ----------
  const ray = new THREE.Raycaster();
  const mouse = new THREE.Vector2();
  let down = null;
  renderer.domElement.addEventListener('pointerdown', e => { down = [e.clientX, e.clientY]; });
  renderer.domElement.addEventListener('pointerup', e => {
    if (!down || Math.hypot(e.clientX - down[0], e.clientY - down[1]) > 6) return;
    const r = renderer.domElement.getBoundingClientRect();
    mouse.set(((e.clientX - r.left) / r.width) * 2 - 1, -((e.clientY - r.top) / r.height) * 2 + 1);
    ray.setFromCamera(mouse, camera);
    const hit = ray.intersectObjects(pickables, false)[0];
    if (hit) onSelect(hit.object.userData.hall);
  });
  renderer.domElement.addEventListener('pointermove', e => {
    const r = renderer.domElement.getBoundingClientRect();
    mouse.set(((e.clientX - r.left) / r.width) * 2 - 1, -((e.clientY - r.top) / r.height) * 2 + 1);
    ray.setFromCamera(mouse, camera);
    renderer.domElement.style.cursor = ray.intersectObjects(pickables, false).length ? 'pointer' : 'grab';
  });

  // ---------- loop ----------
  let insets = { left: 0, right: 0, top: 0, bottom: 0 };
  function resize() {
    const w = container.clientWidth, h = container.clientHeight;
    renderer.setSize(w, h, false);
    // Shift the picture so its center lands in the middle of the area the panels leave open.
    const dx = ((insets.left || 0) - (insets.right || 0)) / 2;
    const dy = ((insets.bottom || 0) - (insets.top || 0)) / 2;
    if (dx || dy) camera.setViewOffset(w, h, -dx, dy, w, h);
    else camera.clearViewOffset();
    // Zoom out in step with how much of the view the panels leave open.
    const openW = Math.max(1, w - (insets.left || 0) - (insets.right || 0));
    const openH = Math.max(1, h - (insets.top || 0) - (insets.bottom || 0));
    camera.zoom = Math.max(0.7, Math.min(1, Math.min(openW / w, openH / h) * 1.6));
    camera.aspect = w / Math.max(1, h);
    // Narrow screens need a wider field of view to fit all three halls.
    camera.fov = camera.aspect < 1 ? 38 + (1 - camera.aspect) * 16 : 38;
    camera.updateProjectionMatrix();
  }
  new ResizeObserver(resize).observe(container);
  resize();
  applyTheme(theme);
  if (document.fonts && document.fonts.ready) {
    document.fonts.ready.then(() => {
      for (const h of Object.values(halls)) h.label.userData.reset();
      if (state) paintHalls();
    });
  }
  matchMedia('(prefers-color-scheme: dark)').addEventListener('change', e => applyTheme(e.matches ? 'dark' : 'light'));

  let frames = 0, fpsT = performance.now(), slowSeconds = 0, running = true;
  const ease = t => 1 - Math.pow(1 - t, 3);
  function frame(now) {
    if (!running) return;
    requestAnimationFrame(frame);
    if (flight) {
      const t = Math.min(1, (now - flight.t0) / flight.dur);
      camera.position.lerpVectors(flight.from, flight.to, ease(t));
      controls.target.lerpVectors(flight.fromT, flight.toT, ease(t));
      if (t >= 1) flight = null;
    }
    controls.update();
    if (!reducedMotion) {
      drawPulses();
      for (const h of Object.values(halls)) {
        if (h.ring.visible) {
          h.ring.rotation.z = now / 2400;
          const s = h.state && h.state.role === 'candidate' ? 1 + 0.25 * Math.sin(now / 120) : 1;
          h.ring.scale.setScalar(s);
        }

        // Rack LEDs: a few flicker each frame while the hall has power.
        const alive = h.state ? h.state.alive : true;
        for (let k = 0; k < 2; k++) {
          const i = (Math.random() * h.leds.count) | 0;
          const hex = !alive ? (theme === 'light' ? 0x5e5a53 : 0x1b1a18) : (Math.random() < 0.06 ? P.ledAlt : P.led);
          h.leds.setColorAt(i, color.set(hex).multiplyScalar(alive ? 0.7 + Math.random() * 0.3 : 1));
        }
        h.leds.instanceColor.needsUpdate = true;
        // Unsaved orders flicker in their bays.
        if (h.state && h.state.alive && h.state.mem > 0) {
          const start = Math.min(h.state.disk, BAYS);
          const on = Math.sin(now / 260) > 0;
          for (let i = start; i < Math.min(BAYS, start + h.state.mem); i++) {
            h.bays.setColorAt(i, color.set(on ? P.bayMem : P.bayEmpty));
          }
          h.bays.instanceColor.needsUpdate = true;
        }
      }
      for (let i = ripples.length - 1; i >= 0; i--) {
        const rp = ripples[i];
        const t = (now - rp.born) / 2800;
        if (t >= 1) {
          scene.remove(rp.m);
          rp.m.geometry.dispose();
          rp.m.material.dispose();
          ripples.splice(i, 1);
          continue;
        }
        rp.m.scale.setScalar(1 + t * 7);
        rp.m.material.opacity = 0.9 * (1 - t);
      }
    }
    renderer.render(scene, camera);

    // Drop resolution if the device can't keep up.
    frames++;
    if (now - fpsT > 1000) {
      const fps = frames * 1000 / (now - fpsT);
      frames = 0;
      fpsT = now;
      slowSeconds = fps < 40 ? slowSeconds + 1 : 0;
      if (slowSeconds >= 2 && pixelRatio > 1) {
        pixelRatio = Math.max(1, pixelRatio - 0.5);
        renderer.setPixelRatio(pixelRatio);
        resize();
        slowSeconds = 0;
      }
    }
  }
  requestAnimationFrame(frame);
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) running = false;
    else if (!running) { running = true; requestAnimationFrame(frame); }
  });

  return {
    update,
    fly,
    /** Pixels covered by panels on each side of the view. */
    setInsets(next) {
      insets = { left: 0, right: 0, top: 0, bottom: 0, ...next };
      resize();
    },
    /** A colored ring on the ground under a hall, for "confirmed" moments. */
    ripple(hallId, policy) {
      const def = HALLS[hallId];
      if (!def || reducedMotion) return;
      ripple(def.pos, policy === 'baseline' ? P.baseline : P.concord);
    },
  };
}
