Vítej na onsite assignmentu!

Téma token discovery aktivně řešíme a proto jsme jako zadání vybrali právě tohle.

### Zadání

Chceme implementovat nové networky rychleji, a proto cesta nového networku nemůže ve výchozím nastavení začínat na Blockbook instanci (nákup HW, instalace serveru atd.).

Řešením je přistupovat k datům a interagovat s blockchainem pomocí Direct RPC. Trezor Suite tak posílá JSON-RPC požadavky přímo na node, který poskytuje JSON-RPC rozhraní.

Jeden z mnoha problémů nastává u non-rich JSON-RPC, které neumí dělat token discovery.

**Cílem je tedy napsat PoC backendové služby, která:**

- pro jednoduchost bere jako zdroj pravdy data na ETH mainnet od bloku 25 933 634 do aktuálního bloku, tzn. umí fungovat v módu skoro „realtime“ parsování
- volba technologií (jazyka, frameworku, databáze) je na tobě, očekáváme však zdůvodnění co a proč si vybral
- obsahuje REST API nebo GraphQL endpoint pro získání všech adres tokenů (ERC-20), se kterými poskytnutá adresa účtu interagovala
    - obsahuje i parametr pro poskytnutí aktuálního zůstatku účtu na adrese tokenu
    - odpověď by měla obsahovat výšku bloku, pro který jsou tato data platná
- je z hlediska datového modelu/contractu a architektury codebase do jisté míry modulární, aby při implementaci dalších networků šel použít např. Adapter pattern (na patternu nezáleží, cílem je, aby codebase měla nějaký modulární základ)
- služba by měla být škálovatelná (schopná obsluhovat 5k uživatelů, kteří zjišťují své zůstatky 1× za minutu)
- služba by měla počítat s tím, že RPC není spolehlivé - může být pomalé, vracet chyby, rate-limitovat nebo být pozadu za chain-tipem. Popiš, jak to tvoje řešení řeší
- obsahuje README a dokumentaci limitů
- není full indexer
- protože se za každý request platí (například v případě Quicknode), měla by být služba efektivní co se týče počtu requestů na RPC
- **BONUS**: ERC721 discovery + nový endpoint

### Poznámky

- udělej si na to prosím vlastní repozitář, který nám nasdílíš
- na závěr onsite assignmentu budeme rádi, když nám své řešení odprezentuješ – v miniprezentaci (max 30min + Q&A) vysvětli svá klíčová rozhodnutí: co a jak jsi udělal; konkrétní termín si společně domluvíme
- na rovinu – v dnešní AI době nevíme, zda je tento scope na PoC moc, nebo málo, proto jsme přidali i „bonus“ item
- transparentnost – není zde „správné“ řešení, a proto se primárně soustředíme na to, jaký postup jsi zvolil a z jakého důvodu
- Tali ti bude moct v případě potřeby nasdílet Quicknode RPC API