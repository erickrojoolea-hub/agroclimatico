"""
agro_database.py
================
Base de datos estática para agricultura chilena.
Contiene información curada de enfermedades, plagas, fertilización,
costos y tecnología de drones para las principales especies frutícolas
de la zona central de Chile.

Fuentes de referencia: ODEPA, INIA, CIREN, SAG, FIA.
"""

# =============================================================================
# 1. ENFERMEDADES Y PLAGAS POR ESPECIE
# =============================================================================

PLAGAS_ENFERMEDADES = {
    "VID": {
        "enfermedades": [
            {
                "nombre": "Oídio de la vid",
                "agente": "Erysiphe necator (Uncinula necator)",
                "sintomas": "Polvo blanquecino-grisáceo en hojas, brotes y racimos. Deformación de bayas, rajado de frutos.",
                "epoca_riesgo": "Septiembre a febrero, favorecido por temperaturas 20-27°C y alta humedad relativa",
                "control": "Azufre mojable (3-5 kg/ha), tebuconazol, miclobutanil, metrafenona. Aplicaciones preventivas desde brotación."
            },
            {
                "nombre": "Botrytis (pudrición gris)",
                "agente": "Botrytis cinerea",
                "sintomas": "Pudrición gris en racimos, micelio gris sobre bayas. Pérdida total de racimos en ataques severos.",
                "epoca_riesgo": "Floración y pinta a cosecha, favorecido por lluvias y humedad >80%",
                "control": "Iprodione, fenhexamid, ciprodinil + fludioxonil, pyrimethanil. Manejo de canopia para ventilación. Deshoje en zona de racimos."
            },
            {
                "nombre": "Mildiú de la vid",
                "agente": "Plasmopara viticola",
                "sintomas": "Manchas aceitosas en el haz de hojas, eflorescencia blanca en envés. Desecación de racimos.",
                "epoca_riesgo": "Primavera con lluvias >10 mm y temperaturas >10°C",
                "control": "Mancozeb, metalaxil + mancozeb, fosetil aluminio, mandipropamid. Aplicar preventivamente antes de lluvias."
            },
            {
                "nombre": "Eutipiosis",
                "agente": "Eutypa lata",
                "sintomas": "Brotes raquíticos con hojas pequeñas y cloróticas. Necrosis interna de madera en corte transversal.",
                "epoca_riesgo": "Infección por heridas de poda en invierno con lluvias",
                "control": "Proteger heridas de poda con pasta poda (tebuconazol). Eliminar madera enferma. Podar en tiempo seco."
            },
        ],
        "plagas": [
            {
                "nombre": "Arañita roja (Panonychus ulmi)",
                "tipo": "Ácaro",
                "daño": "Punteado clorótico en hojas, bronceado foliar, defoliación anticipada. Reduce acumulación de azúcar en bayas.",
                "epoca": "Primavera-verano, desde noviembre",
                "control": "Abamectina, spirodiclofen, etoxazol. Aceite mineral en dormancia para huevos invernantes."
            },
            {
                "nombre": "Chanchito blanco (Pseudococcus spp.)",
                "tipo": "Insecto chupador (Pseudococcidae)",
                "daño": "Fumagina por mielecilla, contaminación de racimos, vector de virus (GLRaV). Rechazo en exportación.",
                "epoca": "Desde brotación, máxima población en verano (diciembre-marzo)",
                "control": "Clorpirifos en dormancia, imidacloprid, spirotetramat, acetamiprid. Control biológico con Cryptolaemus montrouzieri."
            },
            {
                "nombre": "Polilla del racimo (Lobesia botrana)",
                "tipo": "Lepidóptero",
                "daño": "Larvas perforan bayas, facilitando entrada de Botrytis. Plaga cuarentenaria.",
                "epoca": "3 generaciones: octubre, diciembre-enero, febrero-marzo",
                "control": "Monitoreo con trampas de feromona. Clorantraniliprole, metoxifenocide, Bacillus thuringiensis. Confusión sexual."
            },
            {
                "nombre": "Trips de la vid (Frankliniella occidentalis)",
                "tipo": "Insecto raspador-chupador",
                "daño": "Russet en bayas, daño cosmético que reduce categoría de exportación.",
                "epoca": "Floración a cuaja (octubre-noviembre)",
                "control": "Spinosad, spinetoram, acrinatrin. Aplicar en floración."
            },
        ],
    },

    "CEREZO": {
        "enfermedades": [
            {
                "nombre": "Cáncer bacterial",
                "agente": "Pseudomonas syringae pv. syringae / pv. morsprunorum",
                "sintomas": "Cancros en tronco y ramas con exudado gomoso. Muerte de yemas florales, necrosis de brotes en primavera.",
                "epoca_riesgo": "Otoño-invierno, infección por heridas de poda y heladas que dañan tejido",
                "control": "Cobre (oxicloruro, hidróxido) en otoño post-cosecha y pre-invierno. Evitar poda en períodos húmedos. Portainjertos tolerantes (Colt, MaxMa 14)."
            },
            {
                "nombre": "Moniliosis (pudrición morena)",
                "agente": "Monilinia laxa / M. fructicola",
                "sintomas": "Tizón de flores, pudrición parda de frutos con esporulación gris. Momificación de frutos.",
                "epoca_riesgo": "Floración (septiembre) y pre-cosecha, favorecido por lluvia y humedad",
                "control": "Iprodione, tebuconazol, ciprodinil + fludioxonil. Aplicar en botón blanco, plena flor y pre-cosecha."
            },
            {
                "nombre": "Plateado (Silver leaf)",
                "agente": "Chondrostereum purpureum",
                "sintomas": "Hojas con brillo plateado por separación de epidermis. Necrosis interna de madera con zona oscura.",
                "epoca_riesgo": "Infección por heridas de poda en otoño-invierno",
                "control": "Proteger heridas de poda. Trichoderma como antagonista biológico. Eliminar ramas afectadas."
            },
            {
                "nombre": "Pudrición ácida (Sour rot)",
                "agente": "Complejo de levaduras y bacterias acéticas",
                "sintomas": "Frutos blandos con olor avinagrado, exudación de jugo. Asociado a heridas por insectos y lluvia.",
                "epoca_riesgo": "Pre-cosecha, especialmente con lluvias tardías",
                "control": "Coberturas plásticas (rain cover), manejo de insectos vectores (Drosophila). No hay fungicida específico eficaz."
            },
        ],
        "plagas": [
            {
                "nombre": "Drosophila de alas manchadas (Drosophila suzukii)",
                "tipo": "Díptero",
                "daño": "Oviposición en frutos sanos en maduración. Larvas causan colapso y pudrición de frutos. Plaga clave en cereza.",
                "epoca": "Desde pinta a cosecha (noviembre-enero)",
                "control": "Trampeo masivo, spinosad, cipermetrina. Cosecha oportuna, eliminación de fruta remanente. Mallas anti-insectos."
            },
            {
                "nombre": "Pulgón negro del cerezo (Myzus cerasi)",
                "tipo": "Insecto chupador (Áfido)",
                "daño": "Enrollamiento de hojas apicales, fumagina por mielecilla, debilitamiento de brotes.",
                "epoca": "Primavera (septiembre-noviembre)",
                "control": "Imidacloprid, acetamiprid, pirimicarb, flonicamid. Control biológico: Aphidius colemani."
            },
            {
                "nombre": "Escama de San José (Diaspidiotus perniciosus)",
                "tipo": "Insecto chupador (Diaspididae)",
                "daño": "Debilitamiento general del árbol, manchas rojas en frutos que causan rechazo en exportación.",
                "epoca": "Todo el año, dos generaciones (octubre y enero)",
                "control": "Aceite mineral en dormancia (2-3%), clorpirifos, spirotetramat en temporada."
            },
            {
                "nombre": "Gusano del cerezo (Rhagoletis cerasi)",
                "tipo": "Díptero (Tephritidae)",
                "daño": "Larvas se desarrollan dentro del fruto causando pudrición. Plaga cuarentenaria para algunos mercados.",
                "epoca": "Desde pinta (noviembre-diciembre)",
                "control": "Monitoreo con trampas amarillas. Dimetoato, spinosad. Cosecha temprana."
            },
        ],
    },

    "NOGAL": {
        "enfermedades": [
            {
                "nombre": "Peste negra del nogal (Tizón bacteriano)",
                "agente": "Xanthomonas arboricola pv. juglandis",
                "sintomas": "Manchas negras hundidas en frutos, hojas y amentos. Aborto de frutos, necrosis de nuez.",
                "epoca_riesgo": "Primavera con lluvias, desde floración hasta cuaja",
                "control": "Cobre (Bordeaux, hidróxido de cobre) preventivo. Kasugamicina. Aplicar desde inicio de floración femenina. Variedades menos susceptibles: Chandler."
            },
            {
                "nombre": "Agallas de corona (Crown gall)",
                "agente": "Agrobacterium tumefaciens",
                "sintomas": "Tumores en zona del cuello y raíces. Reducción de vigor, clorosis general.",
                "epoca_riesgo": "Infección por heridas en raíces durante plantación o labores de suelo",
                "control": "Usar plantas certificadas libres de agrobacterium. Agrobacterium radiobacter K84 como biocontrolador. Evitar heridas en raíces."
            },
            {
                "nombre": "Antracnosis del nogal",
                "agente": "Gnomonia leptostyla (Ophiognomonia leptostyla)",
                "sintomas": "Manchas circulares pardas en hojas, defoliación prematura. Manchas en frutos.",
                "epoca_riesgo": "Primavera-verano lluvioso",
                "control": "Mancozeb, clorotalonil, cobre. Retirar hojarasca del suelo."
            },
            {
                "nombre": "Pudrición de Phytophthora",
                "agente": "Phytophthora cinnamomi / P. citrophthora",
                "sintomas": "Decaimiento general, clorosis, pudrición de raíces y cuello. Exudado oscuro en base del tronco.",
                "epoca_riesgo": "Suelos con mal drenaje, exceso de riego",
                "control": "Fosetil aluminio, metalaxil. Mejorar drenaje. Portainjertos tolerantes (Paradox)."
            },
        ],
        "plagas": [
            {
                "nombre": "Polilla del nogal (Cydia pomonella)",
                "tipo": "Lepidóptero",
                "daño": "Larvas penetran el fruto y se alimentan de la nuez. Pérdida directa de producción.",
                "epoca": "Dos generaciones: noviembre-diciembre y febrero-marzo",
                "control": "Confusión sexual, clorantraniliprole, metoxifenocide. Trampeo con feromona para monitoreo. Virus de la granulosis (CpGV)."
            },
            {
                "nombre": "Arañita bimaculada (Tetranychus urticae)",
                "tipo": "Ácaro",
                "daño": "Punteado clorótico, bronceado foliar, defoliación. Reduce calibre de nuez.",
                "epoca": "Verano (diciembre-marzo), favorecido por calor y polvo",
                "control": "Abamectina, spirodiclofen, bifenazate. Manejo de polvo en caminos."
            },
            {
                "nombre": "Escama de San José (Diaspidiotus perniciosus)",
                "tipo": "Insecto chupador",
                "daño": "Debilitamiento, manchas en frutos.",
                "epoca": "Todo el año",
                "control": "Aceite mineral en dormancia, spirotetramat en temporada."
            },
            {
                "nombre": "Pulgón del nogal (Chromaphis juglandicola)",
                "tipo": "Insecto chupador",
                "daño": "Fumagina sobre hojas y frutos por mielecilla. Reduce fotosíntesis.",
                "epoca": "Primavera-verano",
                "control": "Imidacloprid, flonicamid. Enemigos naturales: Chrysoperla."
            },
        ],
    },

    "PALTO": {
        "enfermedades": [
            {
                "nombre": "Tristeza del palto (Pudrición de raíces)",
                "agente": "Phytophthora cinnamomi",
                "sintomas": "Marchitez, clorosis progresiva, defoliación, muerte de raicillas. Frutos pequeños. Muerte del árbol en casos severos.",
                "epoca_riesgo": "Todo el año, agravado por exceso de agua y mal drenaje",
                "control": "Fosetil aluminio (inyección al tronco o foliar), metalaxil en riego. Portainjerto tolerante (Duke 7, Dusa). Camellones altos, buen drenaje. Mulch orgánico para promover microbiota supresiva."
            },
            {
                "nombre": "Antracnosis",
                "agente": "Colletotrichum gloeosporioides",
                "sintomas": "Manchas circulares oscuras en frutos maduros, pudrición en postcosecha.",
                "epoca_riesgo": "Pre-cosecha y postcosecha, favorecido por humedad y lluvias",
                "control": "Prochloraz, azoxistrobina en pre-cosecha. Cobre preventivo. Manejo de carga frutal para evitar estrés."
            },
            {
                "nombre": "Fusariosis (Cancrosis del tronco)",
                "agente": "Fusarium sp. / Neonectria sp.",
                "sintomas": "Cancros en tronco con exudado blanquecino, decaimiento sectorial del árbol.",
                "epoca_riesgo": "Heridas mecánicas o por helada",
                "control": "Evitar heridas. Proteger con pasta poda. Cirugía de cancros en casos leves."
            },
            {
                "nombre": "Esclerotiniosis (Pudrición del pedúnculo)",
                "agente": "Sclerotinia sclerotiorum",
                "sintomas": "Pudrición blanda desde pedúnculo, micelio blanco algodonoso.",
                "epoca_riesgo": "Invierno-primavera húmeda",
                "control": "Iprodione, boscalid. Eliminar restos de poda del suelo."
            },
        ],
        "plagas": [
            {
                "nombre": "Trips del palto (Heliothrips haemorrhoidalis)",
                "tipo": "Insecto raspador-chupador (Thripidae)",
                "daño": "Russet y bronceado de frutos que reduce calidad de exportación. Daño cosmético severo.",
                "epoca": "Primavera-verano (septiembre a marzo), mayor presión en zona costera",
                "control": "Spinosad, spinetoram, abamectina. Monitoreo con trampas azules. Control biológico con Orius spp."
            },
            {
                "nombre": "Arañita del palto (Oligonychus yothersi)",
                "tipo": "Ácaro",
                "daño": "Bronceado de hojas, telaraña en envés foliar. Defoliación en ataques severos.",
                "epoca": "Verano-otoño (enero-mayo)",
                "control": "Abamectina, spirodiclofen. Control biológico con Stethorus histrio y Phytoseiulus persimilis."
            },
            {
                "nombre": "Chanchito blanco (Pseudococcus longispinus)",
                "tipo": "Insecto chupador",
                "daño": "Fumagina, contaminación de frutos. Problema cuarentenario para exportación.",
                "epoca": "Primavera-verano",
                "control": "Clorpirifos, imidacloprid, spirotetramat. Cryptolaemus montrouzieri."
            },
            {
                "nombre": "Escama latania (Hemiberlesia lataniae)",
                "tipo": "Insecto chupador (Diaspididae)",
                "daño": "Manchas en frutos, debilitamiento general del árbol.",
                "epoca": "Todo el año, 2-3 generaciones",
                "control": "Aceite mineral, spirotetramat, buprofezin."
            },
        ],
    },

    "ALMENDRO": {
        "enfermedades": [
            {
                "nombre": "Moniliosis (Tizón de flores y pudrición morena)",
                "agente": "Monilinia laxa / M. fructicola",
                "sintomas": "Tizón de flores y brotes, pudrición de frutos con esporulación parda-grisácea. Cancros en ramillas.",
                "epoca_riesgo": "Floración (julio-agosto para almendro) con lluvias o alta humedad",
                "control": "Iprodione, tebuconazol, fenhexamid. Aplicar en botón rosado y plena flor. Eliminar momias."
            },
            {
                "nombre": "Fusicoccum (Chancro de ramas)",
                "agente": "Fusicoccum amygdali (Botryosphaeria dothidea)",
                "sintomas": "Cancros alargados en ramas con exudado gomoso. Muerte regresiva de brotes. Gomosis.",
                "epoca_riesgo": "Estrés hídrico o por heridas, verano-otoño",
                "control": "Poda sanitaria de ramas afectadas. Proteger cortes de poda. Tebuconazol, cobre post-cosecha."
            },
            {
                "nombre": "Roya del almendro",
                "agente": "Tranzschelia discolor",
                "sintomas": "Pústulas anaranjadas en envés de hojas. Defoliación prematura que afecta acumulación de reservas.",
                "epoca_riesgo": "Verano-otoño (febrero-abril)",
                "control": "Tebuconazol, azoxistrobina, mancozeb. Aplicar preventivamente post-cosecha."
            },
            {
                "nombre": "Abolladura (Torque)",
                "agente": "Taphrina deformans",
                "sintomas": "Deformación, engrosamiento y enrojecimiento de hojas jóvenes. Caída prematura de hojas.",
                "epoca_riesgo": "Brotación con lluvias y temperaturas frescas (14-20°C)",
                "control": "Cobre en caída de hojas y pre-brotación. Ziram. Aplicación preventiva antes de hinchamiento de yemas."
            },
        ],
        "plagas": [
            {
                "nombre": "Capachito de los frutales (Naupactus xanthographus)",
                "tipo": "Coleóptero (Curculionidae)",
                "daño": "Adultos cortan hojas en forma de media luna. Larvas en suelo dañan raíces.",
                "epoca": "Adultos activos en primavera-verano (octubre-enero)",
                "control": "Bandas pegajosas en tronco para impedir subida de adultos. Clorpirifos al suelo para larvas. Beauveria bassiana."
            },
            {
                "nombre": "Polilla del almendro (Anarsia lineatella)",
                "tipo": "Lepidóptero",
                "daño": "Larvas barrenan brotes y frutos. Galerías en brotes nuevos.",
                "epoca": "Primavera (dos generaciones: septiembre y diciembre)",
                "control": "Clorantraniliprole, metoxifenocide. Monitoreo con trampas de feromona."
            },
            {
                "nombre": "Pulgón verde del duraznero (Myzus persicae)",
                "tipo": "Insecto chupador",
                "daño": "Enrollamiento de hojas, fumagina, transmisión de virus.",
                "epoca": "Primavera (septiembre-noviembre)",
                "control": "Imidacloprid, pirimicarb, flonicamid. Aceite mineral en dormancia."
            },
            {
                "nombre": "Arañita roja europea (Panonychus ulmi)",
                "tipo": "Ácaro",
                "daño": "Bronceado foliar, defoliación prematura.",
                "epoca": "Verano",
                "control": "Abamectina, spirodiclofen. Aceite mineral en dormancia para huevos."
            },
        ],
    },

    "ARANDANO": {
        "enfermedades": [
            {
                "nombre": "Botrytis (Pudrición gris)",
                "agente": "Botrytis cinerea",
                "sintomas": "Pudrición gris de frutos en pre y postcosecha. Micelio gris sobre bayas. Principal causa de pérdida en postcosecha.",
                "epoca_riesgo": "Floración y cosecha, favorecido por lluvias y humedad alta",
                "control": "Fenhexamid, ciprodinil + fludioxonil, pyrimethanil, boscalid. Aplicar en floración (20%, 50%, 80% flor) y pre-cosecha."
            },
            {
                "nombre": "Pudrición de raíces por Phytophthora",
                "agente": "Phytophthora cinnamomi / P. cryptogea",
                "sintomas": "Clorosis, marchitez, enrojecimiento prematuro de hojas. Pudrición de raíces. Muerte de plantas.",
                "epoca_riesgo": "Suelos mal drenados, exceso de riego, todo el año",
                "control": "Fosetil aluminio, metalaxil. Camellones altos, sustrato con corteza de pino (pH 4.5-5.5). Buen drenaje."
            },
            {
                "nombre": "Antracnosis",
                "agente": "Colletotrichum acutatum",
                "sintomas": "Pudrición blanda anaranjada en frutos maduros. Infección latente que se expresa en postcosecha.",
                "epoca_riesgo": "Primavera-verano, con humedad y temperatura >20°C",
                "control": "Azoxistrobina, piraclostrobin, difenoconazol. Rotación de fungicidas para evitar resistencia."
            },
            {
                "nombre": "Cáncer del tallo (Botryosphaeria)",
                "agente": "Botryosphaeria dothidea / Neofusicoccum spp.",
                "sintomas": "Cancros en tallos, muerte regresiva de cañas. Decaimiento general.",
                "epoca_riesgo": "Estrés, heridas de poda",
                "control": "Poda sanitaria, proteger cortes. Cobre post-poda."
            },
        ],
        "plagas": [
            {
                "nombre": "Drosophila de alas manchadas (Drosophila suzukii)",
                "tipo": "Díptero",
                "daño": "Oviposición en frutos sanos en pinta. Colapso y pudrición de frutos. Plaga clave en arándano.",
                "epoca": "Desde pinta a cosecha (noviembre-febrero)",
                "control": "Spinosad, cipermetrina, delegado. Trampeo masivo. Cosecha frecuente, frío rápido postcosecha. Mallas exclusión."
            },
            {
                "nombre": "Trips del arándano (Frankliniella occidentalis)",
                "tipo": "Insecto raspador-chupador",
                "daño": "Daño en flores que reduce cuaja. Cicatrices en frutos.",
                "epoca": "Floración (septiembre-noviembre)",
                "control": "Spinosad, spinetoram. Monitoreo con trampas azules."
            },
            {
                "nombre": "Gusano del arándano (Proeulia spp.)",
                "tipo": "Lepidóptero (enrollador de hojas)",
                "daño": "Larvas enrollan hojas y dañan frutos. Contaminante en fruta de exportación.",
                "epoca": "Primavera-verano",
                "control": "Bacillus thuringiensis, clorantraniliprole, metoxifenocide."
            },
            {
                "nombre": "Chanchito blanco (Pseudococcus spp.)",
                "tipo": "Insecto chupador",
                "daño": "Contaminante cuarentenario. Fumagina.",
                "epoca": "Primavera-verano",
                "control": "Clorpirifos, spirotetramat. Cryptolaemus montrouzieri."
            },
        ],
    },

    "FRAMBUESO": {
        "enfermedades": [
            {
                "nombre": "Botrytis (Pudrición gris)",
                "agente": "Botrytis cinerea",
                "sintomas": "Pudrición gris en frutos, micelio sobre drupéolas. Principal problema sanitario del frambueso.",
                "epoca_riesgo": "Floración a cosecha, con lluvia y humedad",
                "control": "Fenhexamid, ciprodinil + fludioxonil, pyrimethanil. Manejo de canopia, poda para ventilación."
            },
            {
                "nombre": "Didymella (Tizón de la caña)",
                "agente": "Didymella applanata",
                "sintomas": "Manchas púrpura en nudos de cañas, debilitamiento. Cañas quebradizas.",
                "epoca_riesgo": "Otoño, con humedad",
                "control": "Cobre post-cosecha, captan. Eliminación de cañas viejas."
            },
            {
                "nombre": "Phytophthora de raíces",
                "agente": "Phytophthora fragariae var. rubi",
                "sintomas": "Marchitez, pudrición de raíces y base de cañas. Muerte de plantas.",
                "epoca_riesgo": "Suelos mal drenados, todo el año",
                "control": "Fosetil aluminio, metalaxil. Camellones, buen drenaje. Variedades tolerantes."
            },
            {
                "nombre": "Virosis (Raspberry bushy dwarf virus - RBDV)",
                "agente": "RBDV y otros virus",
                "sintomas": "Desmoronamiento de frutos (crumbly fruit), reducción de vigor y producción.",
                "epoca_riesgo": "Transmisión por polen, todo el año",
                "control": "Plantas certificadas libres de virus. Eliminar plantas enfermas."
            },
        ],
        "plagas": [
            {
                "nombre": "Drosophila de alas manchadas (Drosophila suzukii)",
                "tipo": "Díptero",
                "daño": "Oviposición en frutos maduros. Colapso de drupéolas.",
                "epoca": "Cosecha (noviembre-marzo)",
                "control": "Spinosad, cipermetrina. Trampeo masivo. Cosecha frecuente."
            },
            {
                "nombre": "Áfido grande de la frambuesa (Amphorophora idaei)",
                "tipo": "Insecto chupador",
                "daño": "Vector de virus (RBDV, Raspberry leaf mottle virus). Deformación de brotes.",
                "epoca": "Primavera-verano",
                "control": "Imidacloprid, pirimicarb. Variedades con resistencia genética al áfido."
            },
            {
                "nombre": "Ácaro de la zarza (Acalitus essigi)",
                "tipo": "Ácaro (eriófido)",
                "daño": "Deformación de drupéolas, frutos con sectores duros y secos (dry berry).",
                "epoca": "Primavera-verano",
                "control": "Azufre, abamectina en brotación."
            },
            {
                "nombre": "Capachito de los frutales (Naupactus xanthographus)",
                "tipo": "Coleóptero",
                "daño": "Adultos comen hojas, larvas dañan raíces.",
                "epoca": "Primavera-verano",
                "control": "Bandas pegajosas, clorpirifos al suelo."
            },
        ],
    },

    "AVELLANO_EUROPEO": {
        "enfermedades": [
            {
                "nombre": "Bacteriosis del avellano (Tizón bacteriano)",
                "agente": "Xanthomonas arboricola pv. corylina",
                "sintomas": "Manchas angulares en hojas, cancros en ramas, necrosis de amentos. Exudado bacteriano.",
                "epoca_riesgo": "Primavera con lluvias, desde brotación",
                "control": "Cobre preventivo en otoño (caída hojas) y primavera (pre-brotación). Poda sanitaria."
            },
            {
                "nombre": "Oidio del avellano",
                "agente": "Phyllactinia guttata",
                "sintomas": "Polvo blanco en envés de hojas, manchas cloróticas en haz.",
                "epoca_riesgo": "Verano-otoño con humedad",
                "control": "Azufre, tebuconazol, penconazol."
            },
            {
                "nombre": "Armillaria (Pudrición de raíz)",
                "agente": "Armillaria mellea",
                "sintomas": "Decaimiento progresivo, hojas pequeñas y cloróticas. Micelio blanco bajo corteza en cuello.",
                "epoca_riesgo": "Suelos con restos de bosque nativo, todo el año",
                "control": "No hay control químico eficaz. Eliminar tocones antes de plantar. Evitar sitios con antecedentes."
            },
            {
                "nombre": "Gloeosporium (Antracnosis)",
                "agente": "Gloeosporium coryli",
                "sintomas": "Manchas pardas irregulares en hojas, defoliación temprana.",
                "epoca_riesgo": "Primavera-verano húmedo",
                "control": "Mancozeb, clorotalonil, cobre."
            },
        ],
        "plagas": [
            {
                "nombre": "Balanino del avellano (Curculio nucum)",
                "tipo": "Coleóptero (Curculionidae)",
                "daño": "Hembra perfora el fruto y deposita huevo. Larva consume la almendra, sale y empupa en suelo.",
                "epoca": "Primavera-verano (noviembre-enero)",
                "control": "Lambda-cihalotrina, tiacloprid. Recolectar frutos caídos. Labores de suelo en otoño para exponer pupas."
            },
            {
                "nombre": "Polilla del avellano (Cydia latiferreana)",
                "tipo": "Lepidóptero",
                "daño": "Larvas barrenan frutos, contaminación con fecas.",
                "epoca": "Verano (diciembre-febrero)",
                "control": "Clorantraniliprole, trampas de feromona para monitoreo."
            },
            {
                "nombre": "Pulgones (Myzocallis coryli, Corylobium avellanae)",
                "tipo": "Insectos chupadores",
                "daño": "Fumagina, debilitamiento de brotes jóvenes.",
                "epoca": "Primavera (septiembre-noviembre)",
                "control": "Imidacloprid, pirimicarb. Generalmente se controlan naturalmente."
            },
            {
                "nombre": "Chinche verde (Nezara viridula)",
                "tipo": "Hemíptero",
                "daño": "Alimentación en frutos causa manchas oscuras (kernel necrosis), sabor amargo.",
                "epoca": "Verano-otoño (enero-abril)",
                "control": "Lambda-cihalotrina, bifentrina. Monitoreo con trampas de feromona."
            },
        ],
    },

    "CIRUELO_JAPONES": {
        "enfermedades": [
            {
                "nombre": "Moniliosis (Pudrición morena)",
                "agente": "Monilinia laxa / M. fructicola",
                "sintomas": "Tizón de flores, pudrición parda de frutos con esporulación. Momificación.",
                "epoca_riesgo": "Floración y pre-cosecha con lluvia y humedad",
                "control": "Iprodione, tebuconazol, ciprodinil + fludioxonil. Aplicar en flor y pre-cosecha."
            },
            {
                "nombre": "Roya del ciruelo",
                "agente": "Tranzschelia discolor",
                "sintomas": "Pústulas anaranjadas en envés, manchas amarillas en haz. Defoliación anticipada.",
                "epoca_riesgo": "Verano-otoño (febrero-mayo)",
                "control": "Tebuconazol, azoxistrobina, mancozeb. Aplicar post-cosecha."
            },
            {
                "nombre": "Abolladura (Torque)",
                "agente": "Taphrina deformans",
                "sintomas": "Deformación, engrosamiento y cambio de color de hojas.",
                "epoca_riesgo": "Brotación con lluvias",
                "control": "Cobre y ziram en caída de hojas y pre-brotación."
            },
            {
                "nombre": "Cáncer bacterial",
                "agente": "Pseudomonas syringae",
                "sintomas": "Cancros en tronco y ramas, exudado gomoso. Muerte de yemas.",
                "epoca_riesgo": "Otoño-invierno, heridas de poda con lluvia",
                "control": "Cobre en otoño, proteger heridas de poda."
            },
        ],
        "plagas": [
            {
                "nombre": "Polilla oriental de la fruta (Grapholita molesta)",
                "tipo": "Lepidóptero",
                "daño": "Larvas barrenan brotes y frutos. Galerías en pulpa.",
                "epoca": "Tres generaciones: octubre, diciembre, febrero",
                "control": "Confusión sexual, clorantraniliprole, metoxifenocide. Trampas de feromona."
            },
            {
                "nombre": "Escama de San José (Diaspidiotus perniciosus)",
                "tipo": "Insecto chupador",
                "daño": "Manchas rojas en frutos, debilitamiento del árbol.",
                "epoca": "Todo el año",
                "control": "Aceite mineral en dormancia, spirotetramat."
            },
            {
                "nombre": "Pulgón verde del duraznero (Myzus persicae)",
                "tipo": "Insecto chupador",
                "daño": "Enrollamiento de hojas, fumagina, transmisión de virus (Sharka/PPV).",
                "epoca": "Primavera",
                "control": "Imidacloprid, pirimicarb, flonicamid."
            },
            {
                "nombre": "Capachito de los frutales (Naupactus xanthographus)",
                "tipo": "Coleóptero",
                "daño": "Corte semicircular en hojas; larvas dañan raíces.",
                "epoca": "Primavera-verano",
                "control": "Bandas pegajosas en tronco, clorpirifos al suelo."
            },
        ],
    },
}


# =============================================================================
# 2. PROGRAMA FITOSANITARIO (CALENDARIO MENSUAL)
# =============================================================================

CALENDARIO_FITOSANITARIO = {
    "VID": {
        1: "Enero: Azufre o tebuconazol para oídio. Botritis pre-cierre racimo (fenhexamid). Monitoreo trips y arañita.",
        2: "Febrero: Botritis pre-cosecha (pyrimethanil). Monitoreo Lobesia (2a-3a gen). Cosecha temprana si hay presión.",
        3: "Marzo: Cosecha. Aplicación post-cosecha de cobre si hay mildiú. Monitoreo chanchito blanco residual.",
        4: "Abril: Post-cosecha: cobre para mildiú y enfermedades de madera. Fertilización post-cosecha.",
        5: "Mayo: Caída de hojas. Aplicación de cobre. Limpieza de restos vegetales.",
        6: "Junio: Dormancia. Poda invernal. Proteger heridas grandes (pasta poda con tebuconazol).",
        7: "Julio: Dormancia. Aceite mineral para escamas y chanchito blanco (2-3% en brotes hinchados).",
        8: "Agosto: Punta verde-brotación: azufre para oídio preventivo. Cobre para excoriosis.",
        9: "Septiembre: Brotación: azufre semanal para oídio. Mancozeb para mildiú si hay lluvia. Monitoreo pulgones.",
        10: "Octubre: Pre-flor a flor: tebuconazol/metrafenona para oídio. Botritis en flor (iprodione). Monitoreo Lobesia 1a gen.",
        11: "Noviembre: Post-cuaja: continuar oídio. Manejo canopia (desbrote, deshoje). Monitoreo arañita y trips.",
        12: "Diciembre: Pinta: fungicidas botritis (ciprodinil+fludioxonil). Monitoreo Lobesia 2a gen. Arañita roja.",
    },
    "CEREZO": {
        1: "Enero: Cosecha. Aplicaciones de insecticida para D. suzukii pre-cosecha. Monitoreo pudrición ácida.",
        2: "Febrero: Post-cosecha: cobre. Fertilización post-cosecha (potasio, calcio).",
        3: "Marzo: Cobre post-cosecha. Monitoreo escama de San José.",
        4: "Abril: Caída hojas: cobre (1a aplicación otoñal contra cáncer bacterial).",
        5: "Mayo: Cobre (2a aplicación). Poda invernal - proteger heridas de poda.",
        6: "Junio: Dormancia. Cobre. Limpieza sanitaria.",
        7: "Julio: Aceite mineral para escamas. Cobre pre-brotación.",
        8: "Agosto: Hinchamiento de yemas: cobre. Prevención Monilinia (iprodione/tebuconazol).",
        9: "Septiembre: Floración: fungicidas anti-Monilinia en botón blanco y plena flor. NO aplicar insecticidas que dañen polinizadores.",
        10: "Octubre: Post-cuaja: insecticidas para pulgón negro si hay umbral. Fungicida para Monilinia.",
        11: "Noviembre: Fruto verde: monitoreo D. suzukii (trampas). Calcio foliar para firmeza. Fungicida pre-pinta.",
        12: "Diciembre: Pinta-cosecha: insecticida para D. suzukii (spinosad). Cosecha oportuna. Coberturas plásticas si hay lluvia.",
    },
    "NOGAL": {
        1: "Enero: Monitoreo polilla del nogal (2a gen). Arañita bimaculada si hay presión. Riego óptimo.",
        2: "Febrero: Monitoreo polilla (trampas). Control arañita si necesario (abamectina). Preparación cosecha.",
        3: "Marzo: Pre-cosecha: no aplicar productos con carencias largas. Cosecha temprana de variedades precoces.",
        4: "Abril: Cosecha. Post-cosecha: cobre para peste negra y antracnosis.",
        5: "Mayo: Caída de hojas: cobre. Limpieza de hojarasca del suelo.",
        6: "Junio: Dormancia. Poda invernal.",
        7: "Julio: Dormancia. Aceite mineral + insecticida para escamas si hay antecedente.",
        8: "Agosto: Pre-brotación: cobre. Preparación de equipos de aplicación.",
        9: "Septiembre: Brotación: 1a aplicación cobre para peste negra (inicio floración femenina).",
        10: "Octubre: Floración: cobre + kasugamicina para peste negra (momento crítico). 2-3 aplicaciones durante floración.",
        11: "Noviembre: Post-cuaja: última aplicación cobre. Monitoreo polilla 1a gen (trampas feromona). Control pulgón si necesario.",
        12: "Diciembre: Crecimiento de fruto: insecticida para polilla si hay capturas. Monitoreo arañita.",
    },
    "PALTO": {
        1: "Enero: Monitoreo trips (trampas azules). Arañita del palto (abamectina si umbral). Riego óptimo.",
        2: "Febrero: Trips y arañita. Aplicación de fosetil-Al si hay antecedente de Phytophthora.",
        3: "Marzo: Fosetil-Al (inyección tronco o foliar). Monitoreo chanchito blanco pre-cosecha.",
        4: "Abril: Cosecha (Hass zona central). Control chanchito blanco pre-cosecha. Cobre post-cosecha.",
        5: "Mayo: Cosecha tardía. Post-cosecha: cobre para antracnosis. Fosetil-Al.",
        6: "Junio: Fosetil-Al (2a aplicación). Poda de formación y sanitaria. Mulch orgánico.",
        7: "Julio: Floración: NO aplicar insecticidas (polinización por abejas). Monitoreo sanitario.",
        8: "Agosto: Floración-cuaja: cuidado con aplicaciones. Fosetil-Al foliar.",
        9: "Septiembre: Post-cuaja: inicio monitoreo trips. Primera aplicación de insecticida si hay presión.",
        10: "Octubre: Trips: spinosad/spinetoram. Fosetil-Al. Monitoreo arañita.",
        11: "Noviembre: Crecimiento fruto: trips, arañita. Cobre preventivo para antracnosis.",
        12: "Diciembre: Crecimiento fruto: continuar monitoreo plagas. Fosetil-Al (4a aplicación anual).",
    },
    "ALMENDRO": {
        1: "Enero: Monitoreo polilla y arañita. Pre-cosecha: respetar carencias.",
        2: "Febrero: Cosecha. Cobre + fungicida post-cosecha para roya y fusicoccum.",
        3: "Marzo: Post-cosecha: cobre para roya. Tebuconazol para fusicoccum. Poda sanitaria.",
        4: "Abril: Cobre caída de hojas. Eliminación de momias y restos.",
        5: "Mayo: Cobre pre-invernal para abolladura y moniliosis. Limpieza de huerto.",
        6: "Junio: Dormancia. Poda invernal.",
        7: "Julio: Floración temprana (según variedad): Monilinia en botón rosado (iprodione). Cobre pre-flor.",
        8: "Agosto: Plena flor: fungicida anti-Monilinia. NO insecticidas (polinizadores). 2a aplicación en flor.",
        9: "Septiembre: Post-cuaja: fungicida final contra Monilinia. Insecticida para pulgón y capachito si hay.",
        10: "Octubre: Crecimiento fruto: monitoreo polilla del almendro. Capachito (bandas pegajosas).",
        11: "Noviembre: Monitoreo arañita. Riego óptimo para llenado de almendra.",
        12: "Diciembre: Crecimiento fruto: arañita, polilla 2a gen si hay. Riego y fertilización.",
    },
    "ARANDANO": {
        1: "Enero: Cosecha: D. suzukii (spinosad 7 días carencia). Botrytis pre-cosecha. Cosecha frecuente.",
        2: "Febrero: Cosecha tardía. Post-cosecha: cobre. Poda post-cosecha.",
        3: "Marzo: Post-cosecha: cobre, poda de cañas viejas. Fertilización post-cosecha.",
        4: "Abril: Cobre caída hojas. Limpieza sanitaria. Fosetil-Al si hay Phytophthora.",
        5: "Mayo: Dormancia parcial. Mantenimiento de riego y pH sustrato.",
        6: "Junio: Dormancia. Poda de limpieza.",
        7: "Julio: Pre-brotación: cobre. Revisión sistema riego y fertirrigación.",
        8: "Agosto: Brotación: monitoreo sanitario. Preparar programa de fungicidas.",
        9: "Septiembre: Floración: Botrytis 1a aplicación (20% flor abierta). Fenhexamid o switch.",
        10: "Octubre: Floración: Botrytis 2a y 3a aplicación (50% y 80% flor). Monitoreo trips. NO insecticidas en flor.",
        11: "Noviembre: Cuaja-pinta: fungicida antracnosis. Inicio monitoreo D. suzukii. Chanchito blanco.",
        12: "Diciembre: Pinta-cosecha: D. suzukii control. Botrytis pre-cosecha. Cosecha frecuente.",
    },
    "FRAMBUESO": {
        1: "Enero: Cosecha: D. suzukii, Botrytis pre-cosecha. Cosecha frecuente, frío rápido.",
        2: "Febrero: Cosecha remontante. D. suzukii. Botrytis.",
        3: "Marzo: Fin cosecha. Post-cosecha: cobre. Poda cañas que produjeron.",
        4: "Abril: Cobre. Tratamiento Didymella (captan). Eliminar cañas enfermas.",
        5: "Mayo: Dormancia. Limpieza sanitaria. Fosetil-Al si hay Phytophthora.",
        6: "Junio: Dormancia. Poda final, amarre cañas nuevas.",
        7: "Julio: Pre-brotación: cobre.",
        8: "Agosto: Brotación: monitoreo áfidos (vectores virus). Azufre para ácaros.",
        9: "Septiembre: Crecimiento: control pulgones (pirimicarb). Monitoreo ácaro eriófido.",
        10: "Octubre: Floración: Botrytis 1a aplicación. NO insecticidas en flor.",
        11: "Noviembre: Cuaja: Botrytis 2a aplicación. Inicio monitoreo D. suzukii.",
        12: "Diciembre: Pinta-cosecha: D. suzukii control. Botrytis pre-cosecha.",
    },
    "AVELLANO_EUROPEO": {
        1: "Enero: Monitoreo chinche verde (trampas feromona). Balanino: trampeo y control.",
        2: "Febrero: Cosecha. Recolectar frutos caídos (balanino). Cobre post-cosecha.",
        3: "Marzo: Post-cosecha: cobre para bacteriosis. Poda sanitaria. Labor suelo para pupas balanino.",
        4: "Abril: Cobre caída de hojas (bacteriosis). Limpieza restos.",
        5: "Mayo: Cobre pre-invernal. Poda invernal.",
        6: "Junio: Dormancia. Floración masculina (amentos) - monitoreo bacteriosis.",
        7: "Julio: Floración femenina. Cobre preventivo bacteriosis. Aceite mineral para escamas.",
        8: "Agosto: Brotación: cobre para bacteriosis. Monitoreo oídio.",
        9: "Septiembre: Crecimiento: fungicida oídio si necesario. Monitoreo pulgones.",
        10: "Octubre: Cuaja fruto: azufre para oídio. Monitoreo plagas.",
        11: "Noviembre: Crecimiento fruto: monitoreo balanino (adultos emergiendo). Lambda-cihalotrina si hay.",
        12: "Diciembre: Llenado fruto: monitoreo polilla, chinche. Control si superan umbral.",
    },
    "CIRUELO_JAPONES": {
        1: "Enero: Pre-cosecha: Monilinia (tebuconazol). Cosecha. Monitoreo polilla oriental.",
        2: "Febrero: Cosecha tardía. Post-cosecha: cobre para roya y cáncer bacterial.",
        3: "Marzo: Post-cosecha: cobre. Roya (tebuconazol). Poda sanitaria.",
        4: "Abril: Cobre caída hojas (cáncer bacterial). Limpieza de momias.",
        5: "Mayo: Cobre + ziram para abolladura. Poda invernal.",
        6: "Junio: Dormancia. Aplicación de cobre.",
        7: "Julio: Aceite mineral para escama de San José. Cobre pre-brotación.",
        8: "Agosto: Hinchamiento yemas: cobre + ziram (abolladura). Monilinia preventivo.",
        9: "Septiembre: Floración: fungicida anti-Monilinia en botón rosado y plena flor.",
        10: "Octubre: Post-cuaja: insecticida polilla oriental (clorantraniliprole). Pulgón si hay.",
        11: "Noviembre: Crecimiento fruto: monitoreo polilla 2a gen. Raleo de frutos.",
        12: "Diciembre: Crecimiento: polilla oriental 3a gen. Pre-cosecha Monilinia.",
    },
}


# =============================================================================
# 3. FERTILIZACION POR ESPECIE
# =============================================================================

FERTILIZACION = {
    "VID": {
        "npk_anual": {"N": 80, "P2O5": 40, "K2O": 120},
        "microelementos": ["Mg", "Zn", "B", "Fe", "Mn"],
        "programa_mensual": [
            "Enero: K foliar para madurez. Ca+B foliar para calidad de baya.",
            "Febrero: K foliar final pre-cosecha. Suspender N.",
            "Marzo: Post-cosecha: NPK base (30% del N anual). Ca+Mg.",
            "Abril: Fertilización otoñal con P y K (50% K anual). Enmiendas calcáreas si pH<6.",
            "Mayo: Incorporar materia orgánica (compost 5-10 ton/ha).",
            "Junio: Sin fertilización. Dormancia.",
            "Julio: Sin fertilización. Dormancia.",
            "Agosto: Inicio fertirrigación: N (urea, nitrato de calcio) dosis bajas.",
            "Septiembre: Brotación: N + P arranque. Fertirrigación semanal. Zn+B foliar.",
            "Octubre: Floración: N moderado + K inicio. B foliar para cuaja.",
            "Noviembre: Cuaja-crecimiento: N + K principal. Mg foliar si hay deficiencia.",
            "Diciembre: Envero: reducir N, aumentar K. Ca foliar para firmeza.",
        ],
        "costo_estimado_usd_ha": 800,
    },
    "CEREZO": {
        "npk_anual": {"N": 120, "P2O5": 50, "K2O": 150},
        "microelementos": ["Ca", "B", "Zn", "Fe", "Mn"],
        "programa_mensual": [
            "Enero: Post-cosecha: N (40% anual) + K. Calcio para reservas.",
            "Febrero: Post-cosecha: continuar N + K. Mg si hay deficiencia.",
            "Marzo: NPK balance otoñal. Enmiendas orgánicas.",
            "Abril: P + K profundo. Ca + Mg si suelo ácido (caliza).",
            "Mayo: Materia orgánica (compost 8-10 ton/ha).",
            "Junio: Sin fertilización.",
            "Julio: Sin fertilización.",
            "Agosto: Inicio fertirrigación: N bajo + Ca. Zn foliar.",
            "Septiembre: Floración: B foliar para cuaja. N moderado. Ca inicio.",
            "Octubre: Cuaja: N + K + Ca (cloruro de calcio 0.5% foliar semanal).",
            "Noviembre: Crecimiento fruto: K alto + Ca foliar semanal (firmeza). Reducir N.",
            "Diciembre: Pre-cosecha: K + Ca. Suspender N 30 días antes de cosecha.",
        ],
        "costo_estimado_usd_ha": 1200,
    },
    "NOGAL": {
        "npk_anual": {"N": 150, "P2O5": 60, "K2O": 100},
        "microelementos": ["Zn", "B", "Mn", "Fe", "Cu"],
        "programa_mensual": [
            "Enero: K fertirrigación. Zn foliar si hay deficiencia.",
            "Febrero: K + Mg. Reducir N.",
            "Marzo: Pre-cosecha: suspender fertirrigación 2 semanas antes.",
            "Abril: Post-cosecha: N + K (30% del total anual).",
            "Mayo: P + K profundo (fósforo otoñal). Materia orgánica.",
            "Junio: Sin fertilización.",
            "Julio: Sin fertilización.",
            "Agosto: Pre-brotación: N inicio (nitrato de amonio o UAN).",
            "Septiembre: Brotación: N + Zn foliar (sulfato de zinc 0.5%). B foliar.",
            "Octubre: Floración-cuaja: N principal + K inicio. B para cuaja.",
            "Noviembre: Crecimiento: N (60% total entre sept-dic) + K + Mg.",
            "Diciembre: Llenado de nuez: K alto. N moderado. Zn + B foliar.",
        ],
        "costo_estimado_usd_ha": 1000,
    },
    "PALTO": {
        "npk_anual": {"N": 180, "P2O5": 50, "K2O": 200},
        "microelementos": ["Zn", "B", "Fe", "Mn", "Cu"],
        "programa_mensual": [
            "Enero: N + K fertirrigación alta. Zn + B foliar.",
            "Febrero: N + K. Fosfito de potasio foliar (Phytophthora + K).",
            "Marzo: K alto pre-cosecha. Reducir N gradualmente.",
            "Abril: Cosecha Hass. Post-cosecha: N + K restauración.",
            "Mayo: Post-cosecha: N + K. Enmiendas orgánicas (mulch).",
            "Junio: N moderado (el palto no tiene dormancia completa). K.",
            "Julio: Floración: B foliar para cuaja. N bajo. K.",
            "Agosto: Floración-cuaja: B + Zn foliar. N moderado.",
            "Septiembre: Post-cuaja: N aumentar + K. Fe-EDDHA si clorosis (suelos calcáreos).",
            "Octubre: Crecimiento fruto: N + K altos. Mg si hay deficiencia.",
            "Noviembre: N + K principales (máxima demanda). Ca + B foliar.",
            "Diciembre: Crecimiento fruto: N + K altos. Zn foliar. Fosfito K.",
        ],
        "costo_estimado_usd_ha": 1400,
    },
    "ALMENDRO": {
        "npk_anual": {"N": 120, "P2O5": 50, "K2O": 100},
        "microelementos": ["Zn", "B", "Fe", "Mn"],
        "programa_mensual": [
            "Enero: K alto para llenado. Reducir N.",
            "Febrero: Pre-cosecha: K. Suspender N.",
            "Marzo: Post-cosecha: N + K (30% anual). Comienzo fertilización otoñal.",
            "Abril: Post-cosecha: N + P + K. Enmiendas orgánicas.",
            "Mayo: P profundo. Materia orgánica.",
            "Junio: Sin fertilización.",
            "Julio: Pre-floración: B foliar si es temprana. N inicio bajo.",
            "Agosto: Floración-cuaja: N + B + Zn foliar.",
            "Septiembre: Post-cuaja: N principal + K inicio.",
            "Octubre: Crecimiento: N + K (máxima absorción).",
            "Noviembre: Llenado almendra: K alto + Mg. N moderado.",
            "Diciembre: K alto. Reducir N gradualmente. Zn foliar.",
        ],
        "costo_estimado_usd_ha": 850,
    },
    "ARANDANO": {
        "npk_anual": {"N": 60, "P2O5": 20, "K2O": 60},
        "microelementos": ["Fe", "Zn", "Mn", "B", "Mg"],
        "programa_mensual": [
            "Enero: K foliar para calidad. Reducir N. Ca foliar firmeza.",
            "Febrero: K. Suspender N. Mg si hay deficiencia.",
            "Marzo: Post-cosecha: N + K (30% anual).",
            "Abril: Fertilización otoñal: N + P + K. Azufre elemental si pH>5.5.",
            "Mayo: Materia orgánica (corteza pino, aserrín compostado). Mantener pH 4.5-5.5.",
            "Junio: Sin fertilización.",
            "Julio: Sin fertilización. Verificar pH agua riego.",
            "Agosto: Inicio fertirrigación: N bajo (sulfato de amonio preferido - acidifica).",
            "Septiembre: Brotación-floración: N + Fe-EDDHA si hay clorosis. B foliar.",
            "Octubre: Floración-cuaja: N + K. B para cuaja.",
            "Noviembre: Crecimiento fruto: N + K. Fe + Mn foliar.",
            "Diciembre: Pinta-cosecha: K + Ca. Reducir N.",
        ],
        "costo_estimado_usd_ha": 700,
    },
    "FRAMBUESO": {
        "npk_anual": {"N": 80, "P2O5": 30, "K2O": 100},
        "microelementos": ["Fe", "Mn", "Zn", "B", "Mg"],
        "programa_mensual": [
            "Enero: Cosecha: K + Ca foliar. Reducir N.",
            "Febrero: Cosecha remontante: K. N bajo.",
            "Marzo: Post-cosecha: N + K. Materia orgánica.",
            "Abril: N + P + K otoñal para reservas en raíces.",
            "Mayo: Compost, corteza pino. Ajuste pH si necesario.",
            "Junio: Sin fertilización.",
            "Julio: Sin fertilización.",
            "Agosto: Inicio fertirrigación: N bajo + Fe-EDDHA.",
            "Septiembre: Brotación: N + K arranque. B + Zn foliar.",
            "Octubre: Floración: N + K + B. Ca foliar.",
            "Noviembre: Cuaja-crecimiento: N + K (máxima demanda). Mg.",
            "Diciembre: Pinta-cosecha: K + Ca. Reducir N.",
        ],
        "costo_estimado_usd_ha": 650,
    },
    "AVELLANO_EUROPEO": {
        "npk_anual": {"N": 100, "P2O5": 40, "K2O": 80},
        "microelementos": ["B", "Zn", "Mn", "Fe"],
        "programa_mensual": [
            "Enero: K para llenado de fruto. Reducir N.",
            "Febrero: Pre-cosecha: K. Suspender fertirrigación antes de cosecha.",
            "Marzo: Post-cosecha: N + K (30% total).",
            "Abril: Fertilización otoñal: N + P + K. Materia orgánica (10-15 ton/ha compost).",
            "Mayo: P profundo. Cal si pH < 6.0.",
            "Junio: Sin fertilización. Floración masculina.",
            "Julio: Floración femenina. B foliar si deficiente.",
            "Agosto: Inicio fertirrigación: N arranque.",
            "Septiembre: Brotación: N + B + Zn foliar.",
            "Octubre: Crecimiento: N + K principales.",
            "Noviembre: Llenado: K alto + N moderado. Mg si deficiente.",
            "Diciembre: Llenado: K. N reducir. B foliar.",
        ],
        "costo_estimado_usd_ha": 750,
    },
    "CIRUELO_JAPONES": {
        "npk_anual": {"N": 100, "P2O5": 40, "K2O": 120},
        "microelementos": ["Ca", "B", "Zn", "Fe", "Mn"],
        "programa_mensual": [
            "Enero: Pre-cosecha: K + Ca foliar para firmeza y color.",
            "Febrero: Post-cosecha: N + K (40% del total anual).",
            "Marzo: Post-cosecha: N + K. Enmiendas orgánicas.",
            "Abril: P + K profundo. Caliza si pH bajo.",
            "Mayo: Materia orgánica. Sin fertirrigación.",
            "Junio: Sin fertilización.",
            "Julio: Sin fertilización.",
            "Agosto: Pre-brotación: N inicio.",
            "Septiembre: Floración: B foliar para cuaja. N + Ca.",
            "Octubre: Cuaja: N + K + Ca. Raleo manual/químico.",
            "Noviembre: Crecimiento fruto: N + K alto. Ca foliar semanal.",
            "Diciembre: Pre-cosecha: K + Ca. Reducir N.",
        ],
        "costo_estimado_usd_ha": 800,
    },
}


# =============================================================================
# 4. COSTOS POR HECTAREA (USD, zona central Chile)
# =============================================================================

COSTOS_HECTAREA = {
    "VID": {
        "establecimiento_usd_ha": 12000,
        "mantencion_anual_usd_ha": 5500,
        "rendimiento_ton_ha": 10.0,
        "precio_fob_usd_kg": 1.80,
        "ingreso_bruto_usd_ha": 18000,
        "margen_estimado_pct": 25,
        "años_hasta_produccion": 3,
        "vida_util_años": 25,
        "notas": "Vid vinífera para exportación. Uva de mesa: establecimiento $18,000/ha, FOB $2.0-2.5/kg, rendimiento 15-20 ton/ha.",
    },
    "CEREZO": {
        "establecimiento_usd_ha": 25000,
        "mantencion_anual_usd_ha": 8000,
        "rendimiento_ton_ha": 10.0,
        "precio_fob_usd_kg": 5.00,
        "ingreso_bruto_usd_ha": 50000,
        "margen_estimado_pct": 35,
        "años_hasta_produccion": 4,
        "vida_util_años": 20,
        "notas": "Alta inversión inicial (cobertura plástica ~$8,000/ha incluida). Requiere frío invernal >800 horas. Cosecha concentrada en 3-4 semanas (nov-ene). Principal destino: China.",
    },
    "NOGAL": {
        "establecimiento_usd_ha": 15000,
        "mantencion_anual_usd_ha": 4500,
        "rendimiento_ton_ha": 5.0,
        "precio_fob_usd_kg": 3.50,
        "ingreso_bruto_usd_ha": 17500,
        "margen_estimado_pct": 28,
        "años_hasta_produccion": 5,
        "vida_util_años": 30,
        "notas": "Variedad principal: Chandler (85% superficie). Chile es 2o exportador mundial. Cosecha mecanizada. Secado en plantas procesadoras.",
    },
    "PALTO": {
        "establecimiento_usd_ha": 18000,
        "mantencion_anual_usd_ha": 6000,
        "rendimiento_ton_ha": 12.0,
        "precio_fob_usd_kg": 2.50,
        "ingreso_bruto_usd_ha": 30000,
        "margen_estimado_pct": 30,
        "años_hasta_produccion": 4,
        "vida_util_años": 25,
        "notas": "Variedad Hass (95%). Sensible a heladas y exceso de agua. Zona principal: V Región (Petorca, La Ligua, Quillota). Mercado principal: Europa y EE.UU.",
    },
    "ALMENDRO": {
        "establecimiento_usd_ha": 12000,
        "mantencion_anual_usd_ha": 4000,
        "rendimiento_ton_ha": 3.5,
        "precio_fob_usd_kg": 4.50,
        "ingreso_bruto_usd_ha": 15750,
        "margen_estimado_pct": 25,
        "años_hasta_produccion": 4,
        "vida_util_años": 25,
        "notas": "Variedades: Nonpareil, Carmel, Independence. Floración temprana (julio-agosto) susceptible a heladas. Cosecha mecanizada. Crecimiento de superficie en Chile.",
    },
    "ARANDANO": {
        "establecimiento_usd_ha": 30000,
        "mantencion_anual_usd_ha": 12000,
        "rendimiento_ton_ha": 10.0,
        "precio_fob_usd_kg": 5.50,
        "ingreso_bruto_usd_ha": 55000,
        "margen_estimado_pct": 30,
        "años_hasta_produccion": 3,
        "vida_util_años": 15,
        "notas": "Alto costo de establecimiento incluye sustrato, fertirriego, malla. Mano de obra intensiva en cosecha. Variedades: Duke, Brigitta, Legacy, Star. Contra-estación para hemisferio norte.",
    },
    "FRAMBUESO": {
        "establecimiento_usd_ha": 15000,
        "mantencion_anual_usd_ha": 8000,
        "rendimiento_ton_ha": 12.0,
        "precio_fob_usd_kg": 3.50,
        "ingreso_bruto_usd_ha": 42000,
        "margen_estimado_pct": 28,
        "años_hasta_produccion": 2,
        "vida_util_años": 10,
        "notas": "Chile principal exportador IQF. Variedades: Heritage (remontante), Meeker, Tulameen. Maule y Biobío zonas principales. Congelado IQF y fresco.",
    },
    "AVELLANO_EUROPEO": {
        "establecimiento_usd_ha": 14000,
        "mantencion_anual_usd_ha": 3500,
        "rendimiento_ton_ha": 3.0,
        "precio_fob_usd_kg": 4.00,
        "ingreso_bruto_usd_ha": 12000,
        "margen_estimado_pct": 22,
        "años_hasta_produccion": 5,
        "vida_util_años": 30,
        "notas": "Variedades: Tonda di Giffoni, Barcelona. Plantaciones concentradas en Maule y Biobío. Ferrero principal comprador. Cosecha mecanizada. Bajo requerimiento hídrico relativo.",
    },
    "CIRUELO_JAPONES": {
        "establecimiento_usd_ha": 12000,
        "mantencion_anual_usd_ha": 5000,
        "rendimiento_ton_ha": 15.0,
        "precio_fob_usd_kg": 1.80,
        "ingreso_bruto_usd_ha": 27000,
        "margen_estimado_pct": 25,
        "años_hasta_produccion": 3,
        "vida_util_años": 20,
        "notas": "Variedades: Angeleno, Larry Ann, Fortune, Black Amber. Requiere raleo intensivo. Mercados: EE.UU., Europa, Asia. O'Higgins y Maule zonas principales.",
    },
}


# =============================================================================
# 5. DRONES EN AGRICULTURA
# =============================================================================

DRONES_INFO = {
    "aplicaciones": [
        {
            "nombre": "Fumigación aérea con drones",
            "descripcion": "Aplicación de fitosanitarios (fungicidas, insecticidas, acaricidas) mediante drones pulverizadores. "
                           "Gotas ultrafinas con volumen de 2-5 L/ha (ultra bajo volumen).",
            "beneficio": "Reducción de 30-50% en uso de agua y agroquímicos. Acceso a terrenos con pendiente. "
                         "Sin compactación de suelo. Operación nocturna posible.",
            "costo_referencial_usd_ha": 25,
        },
        {
            "nombre": "Fertilización foliar aérea",
            "descripcion": "Aplicación de fertilizantes foliares, bioestimulantes y reguladores de crecimiento. "
                           "Ideal para microelementos (Zn, B, Fe, Ca).",
            "beneficio": "Mayor uniformidad que aplicación terrestre. Menor daño mecánico al cultivo. "
                         "Rapidez: 5-8 ha/hora.",
            "costo_referencial_usd_ha": 20,
        },
        {
            "nombre": "Mapeo multiespectral (NDVI)",
            "descripcion": "Vuelo con cámara multiespectral para generar mapas de índices vegetativos (NDVI, NDRE, GNDVI). "
                           "Permite identificar zonas con diferente vigor vegetativo.",
            "beneficio": "Detección temprana de estrés, manejo por zonas (agricultura de precisión), "
                         "optimización de fertilización variable. Monitoreo de evolución del cultivo.",
            "costo_referencial_usd_ha": 15,
        },
        {
            "nombre": "Detección de estrés hídrico",
            "descripcion": "Cámara térmica infrarroja para mapear temperatura de canopia. "
                           "Diferencias de temperatura indican estrés hídrico diferencial.",
            "beneficio": "Optimización de riego, detección de fallas en sistema de riego, "
                         "ahorro de 15-25% en agua. Mapas de CWSI (Crop Water Stress Index).",
            "costo_referencial_usd_ha": 18,
        },
        {
            "nombre": "Conteo de plantas y evaluación de stand",
            "descripcion": "Ortomosaicos de alta resolución (RGB) para conteo automático de plantas, "
                           "medición de cobertura de canopia y detección de fallas de plantación.",
            "beneficio": "Inventario preciso del huerto (>98% exactitud). Detección de plantas faltantes o muertas. "
                         "Planificación de replante.",
            "costo_referencial_usd_ha": 12,
        },
        {
            "nombre": "Monitoreo de plagas y enfermedades",
            "descripcion": "Combinación de imágenes multiespectrales y RGB de alta resolución para "
                           "detectar patrones de infección o ataque de plagas antes de que sean visibles al ojo.",
            "beneficio": "Detección temprana permite aplicaciones focalizadas (spot spraying). "
                         "Reducción de 20-40% en uso de pesticidas. Mejor timing de aplicación.",
            "costo_referencial_usd_ha": 15,
        },
    ],
    "ventajas": [
        "Reducción de 30-50% en volumen de agroquímicos por aplicación de ultra bajo volumen",
        "Acceso a terrenos con pendiente pronunciada (laderas de cerro) donde maquinaria terrestre no puede operar",
        "Sin compactación del suelo ni daño mecánico a infraestructura de riego",
        "Velocidad de operación: 5-10 ha/hora en aplicación, 20-30 ha/hora en mapeo",
        "Operación posible en horarios óptimos (amanecer, atardecer, noche) con mejor eficacia de productos",
        "Precisión de aplicación con RTK: ±2 cm de posicionamiento",
        "Menor exposición del operador a agroquímicos (operación remota)",
        "Datos georreferenciados para agricultura de precisión y trazabilidad",
        "Respuesta rápida ante focos de plagas o enfermedades (spot treatment)",
        "Complemento ideal para certificaciones (GlobalGAP, orgánica) al reducir uso de insumos",
    ],
    "limitaciones": [
        "Autonomía de vuelo limitada: 10-15 minutos por batería en modelos de fumigación",
        "Capacidad de carga: 30-40 litros máximo (DJI Agras T40), requiere múltiples recargas para predios grandes",
        "Sensibilidad al viento: operación limitada con vientos >15 km/h para fumigación",
        "Costo inicial elevado: USD 15,000-25,000 para drones de aplicación profesional",
        "Requiere operador certificado (DGAC Chile) y seguro de responsabilidad civil",
        "Regulación restrictiva en cercanía de aeropuertos y zonas pobladas",
        "No reemplaza aplicaciones de alto volumen (>100 L/ha) requeridas por algunos productos",
        "Limitación en follaje muy denso: penetración de gota puede ser insuficiente sin coadyuvantes adecuados",
        "Dependencia de condiciones meteorológicas para vuelo y aplicación",
        "Requiere infraestructura de carga (generador o red eléctrica en campo)",
    ],
    "equipos_recomendados": [
        {
            "modelo": "DJI Agras T40",
            "uso": "Fumigación y fertilización foliar",
            "capacidad": "40 L tanque líquido / 50 kg dispersión sólida. Ancho de aspersión 11 m. Flujo 6-12 L/min.",
            "precio_referencial": "USD 18,000 - 22,000 (con baterías y cargador)",
            "caracteristicas": "Radar de terreno dual, RTK centimétrico, resistencia IP67. Rendimiento 5-10 ha/hora."
        },
        {
            "modelo": "DJI Agras T25",
            "uso": "Fumigación en predios medianos",
            "capacidad": "25 L tanque líquido / 25 kg dispersión. Compacto y portátil.",
            "precio_referencial": "USD 12,000 - 15,000",
            "caracteristicas": "Más liviano y transportable que T40. Ideal para predios <50 ha. RTK integrado."
        },
        {
            "modelo": "DJI Mavic 3 Multispectral",
            "uso": "Mapeo multiespectral y monitoreo",
            "capacidad": "Cámara RGB 20 MP + 4 bandas multiespectrales (G, R, RE, NIR). Vuelo 43 min.",
            "precio_referencial": "USD 6,500 - 8,000",
            "caracteristicas": "Sensor solar integrado para calibración. Genera NDVI, NDRE, GNDVI. Compacto y fácil de operar. DJI Terra para procesamiento."
        },
        {
            "modelo": "DJI Matrice 350 RTK + cámara térmica Zenmuse H20T",
            "uso": "Detección de estrés hídrico y monitoreo térmico",
            "capacidad": "Cámara térmica 640x512, RGB 20 MP, zoom 200x. Vuelo 55 min.",
            "precio_referencial": "USD 15,000 - 20,000 (drone + payload)",
            "caracteristicas": "Plataforma profesional. Mapeo térmico de alta resolución. Ideal para evaluación de riego."
        },
        {
            "modelo": "DJI Mavic 3 Enterprise",
            "uso": "Inspección general, conteo de plantas, supervisión",
            "capacidad": "Cámara Hasselblad 20 MP + tele 56x. Vuelo 45 min.",
            "precio_referencial": "USD 4,500 - 5,500",
            "caracteristicas": "Compacto, fácil operación. Módulo RTK opcional. Ideal para supervisión diaria del huerto."
        },
    ],
    "normativa_chile": (
        "En Chile, la operación de drones (RPAS - Remotely Piloted Aircraft Systems) está regulada por "
        "la Dirección General de Aeronáutica Civil (DGAC) bajo la DAN 151 (Norma sobre operación de RPAS). "
        "Requisitos principales:\n\n"
        "1. REGISTRO: Todo drone >250 g debe ser registrado en la DGAC.\n"
        "2. CERTIFICACION DEL OPERADOR: Se requiere licencia de piloto remoto emitida por la DGAC. "
        "Para operaciones agrícolas comerciales se necesita categoría 'Operador RPAS Agrícola'.\n"
        "3. PERMISOS DE VUELO: Operaciones VLOS (Visual Line of Sight) hasta 120 m AGL (sobre nivel del suelo). "
        "Para vuelos BVLOS (Beyond Visual Line of Sight) se requiere autorización especial.\n"
        "4. ZONAS RESTRINGIDAS: Prohibido operar a menos de 2 km de aeropuertos/aeródromos sin autorización. "
        "Respetar zonas prohibidas (CTR, TMA) publicadas en carta aeronáutica.\n"
        "5. SEGURO: Obligatorio seguro de responsabilidad civil para operaciones comerciales.\n"
        "6. APLICACION DE FITOSANITARIOS: La aplicación aérea de agroquímicos con drones requiere adicionalmente:\n"
        "   - Inscripción en el SAG (Servicio Agrícola y Ganadero) como aplicador aéreo.\n"
        "   - Receta fitosanitaria emitida por ingeniero agrónomo.\n"
        "   - Registro de aplicaciones (producto, dosis, coordenadas, fecha, hora, condiciones meteorológicas).\n"
        "   - Respetar zonas buffer: 100 m de cursos de agua, 200 m de zonas habitadas.\n"
        "   - Productos autorizados por SAG para aplicación aérea.\n"
        "7. HORARIO: Se recomienda operar en horario diurno. Operación nocturna requiere iluminación del RPAS "
        "y autorización especial.\n"
        "8. METEOROLOGIA: No operar con vientos superiores a 15 km/h para aplicación de fitosanitarios. "
        "No operar bajo lluvia o tormentas eléctricas.\n\n"
        "Fuente: DGAC Chile, DAN 151 vigente. SAG, Resolución Exenta sobre aplicaciones aéreas."
    ),
}


# =============================================================================
# 6. HELPER FUNCTION
# =============================================================================

def get_especie_data(especie_key: str) -> dict:
    """
    Retorna todos los datos disponibles para una especie en un solo diccionario.

    Parameters
    ----------
    especie_key : str
        Clave de la especie en mayúsculas. Valores válidos:
        VID, CEREZO, NOGAL, PALTO, ALMENDRO, ARANDANO,
        FRAMBUESO, AVELLANO_EUROPEO, CIRUELO_JAPONES

    Returns
    -------
    dict con claves:
        - plagas_enfermedades: enfermedades y plagas de la especie
        - calendario_fitosanitario: programa mensual de aplicaciones
        - fertilizacion: plan de fertilización anual y mensual
        - costos: costos de establecimiento, mantención, rendimientos y márgenes
        - especie: nombre de la clave consultada

    Raises
    ------
    ValueError
        Si la especie no existe en la base de datos.
    """
    key = especie_key.upper().strip()

    # Normalizar variantes comunes
    _aliases = {
        "UVA": "VID",
        "VID_VINIFERA": "VID",
        "UVA_DE_MESA": "VID",
        "CHERRY": "CEREZO",
        "CEREZA": "CEREZO",
        "WALNUT": "NOGAL",
        "NUEZ": "NOGAL",
        "AGUACATE": "PALTO",
        "PALTA": "PALTO",
        "AVOCADO": "PALTO",
        "ALMOND": "ALMENDRO",
        "ALMENDRA": "ALMENDRO",
        "BLUEBERRY": "ARANDANO",
        "ARANDANOS": "ARANDANO",
        "ARÁNDANO": "ARANDANO",
        "ARÁNDANOS": "ARANDANO",
        "RASPBERRY": "FRAMBUESO",
        "FRAMBUESA": "FRAMBUESO",
        "HAZELNUT": "AVELLANO_EUROPEO",
        "AVELLANO": "AVELLANO_EUROPEO",
        "PLUM": "CIRUELO_JAPONES",
        "CIRUELA": "CIRUELO_JAPONES",
        "CIRUELO": "CIRUELO_JAPONES",
    }
    key = _aliases.get(key, key)

    especies_validas = list(PLAGAS_ENFERMEDADES.keys())
    if key not in especies_validas:
        raise ValueError(
            f"Especie '{especie_key}' no encontrada. "
            f"Especies válidas: {', '.join(especies_validas)}"
        )

    return {
        "especie": key,
        "plagas_enfermedades": PLAGAS_ENFERMEDADES.get(key, {}),
        "calendario_fitosanitario": CALENDARIO_FITOSANITARIO.get(key, {}),
        "fertilizacion": FERTILIZACION.get(key, {}),
        "costos": COSTOS_HECTAREA.get(key, {}),
    }


def listar_especies() -> list:
    """Retorna lista de todas las especies disponibles en la base de datos."""
    return list(PLAGAS_ENFERMEDADES.keys())


def resumen_costos() -> dict:
    """
    Retorna un resumen comparativo de costos e ingresos de todas las especies.
    Útil para análisis de inversión y comparación entre alternativas.
    """
    resumen = {}
    for especie, datos in COSTOS_HECTAREA.items():
        resumen[especie] = {
            "establecimiento": datos["establecimiento_usd_ha"],
            "mantencion_anual": datos["mantencion_anual_usd_ha"],
            "ingreso_bruto": datos["ingreso_bruto_usd_ha"],
            "margen_pct": datos["margen_estimado_pct"],
            "años_retorno_estimado": round(
                datos["establecimiento_usd_ha"]
                / max(
                    (datos["ingreso_bruto_usd_ha"] * datos["margen_estimado_pct"] / 100)
                    - datos["mantencion_anual_usd_ha"] * 0.1,
                    1,
                ),
                1,
            ),
        }
    return resumen


# =============================================================================
# Si se ejecuta directamente, mostrar resumen
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("BASE DE DATOS AGROCLIMATICA - CHILE")
    print("=" * 60)
    print(f"\nEspecies disponibles: {len(listar_especies())}")
    for esp in listar_especies():
        data = get_especie_data(esp)
        n_enf = len(data["plagas_enfermedades"].get("enfermedades", []))
        n_pla = len(data["plagas_enfermedades"].get("plagas", []))
        costo = data["costos"].get("establecimiento_usd_ha", 0)
        ingreso = data["costos"].get("ingreso_bruto_usd_ha", 0)
        print(
            f"  {esp:25s} | {n_enf} enfermedades, {n_pla} plagas | "
            f"Establ: ${costo:,}/ha | Ingreso: ${ingreso:,}/ha"
        )
    print(f"\nAplicaciones de drones: {len(DRONES_INFO['aplicaciones'])}")
    print(f"Equipos recomendados: {len(DRONES_INFO['equipos_recomendados'])}")
    print("\n[OK] Base de datos cargada correctamente.")
