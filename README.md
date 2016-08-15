# Felles statistikkhåndtering i det offentlige (PoC)

Dette prosjektet inneholder en proof-of-concept for håndtering av statistikkdata fra felleskomponenter.

## Hurtigstart

Prosjektet leverer Java-tjenester pakket inn i Docker-bilder.

### Forutsetninger

Du må ha følgende installert:

* JDK 1.8 eller nyere.
* Maven 3.3 eller nyere.
* [Docker](https://www.docker.com/products/docker-toolbox).

Du trenger også nettverkstilgang til Difi sin [artifakt-brønn](http://eid-artifactory.dmz.local:8080).

### Lag Docker-bilder

Dette bygger Docker-bilder for prosjektets artifakter.

```
$ mvn package
```

### Verifiser at Docker-bildene fungerer som de skal

Følgende kjører et sett med integrasjonstester for å verifisere at Docker-bildene fungerer korrekt.

```
$ mvn verify
```

### Start applikasjonen i ditt lokale Docker-miljø

Slik startes en enkeltinstans av applikasjonen på din lokale Docker-maskin:

```
$ docker run -d --name elasticsearch --restart=unless-stopped elasticsearch:2.3.5
$ docker run -d --name statistics-query --restart=unless-stopped --link elasticsearch -p 8080:8080 docker-registry.dmz.local/statistics-query-elasticsearch:DEV-SNAPSHOT
$ docker run -d --name statistics-ingest --restart=unless-stopped --link elasticsearch -p 8081:8080 docker-registry.dmz.local/statistics-ingest-elasticsearch:DEV-SNAPSHOT
```

Endepunktet til applikasjonen vil da være tilgjengelig på http://$(docker-machine ip).

_Dette forutsetter at port 80 er tilgjengelig i Docker-maskinen din. Hvis ikke kan du endre port-assosiasjonen i
p-flagget ovenfor._

Alternativt kan applikasjonen startes via Maven:

```
$ mvn -pl statistics-api docker:run
```

Merk at i dette tilfellet benyttes dynamisk port-assosiasjon, så du må inspisere konteineren for å utlede endepunktet.

Konteinerne stoppes og fjernes tilsvarende på denne måten:
```
$ mvn -pl statistics-api docker:stop
```

### Start applikasjonen på en Docker-sverm

_Denne beskrivelsen forutsetter at det allerede er satt opp en [Docker-sverm](https://docs.docker.com/engine/swarm) og
at Docker-klienten peker til en manager-node._

Lag først et overlay-nettverk tjenestene kan kommunisere på:
```
$ docker network create -d overlay statistics
```

Start deretter tjenestene _statistics-query_, _statistics-ingest_ og _elasticsearch_. Her startes hver tjeneste med tre
replikaer:
```
$ docker service create --network statistics --replicas 3 --name statistics-query -p 8080:8080 docker-registry.dmz.local/statistics-query-elasticsearch \
  && docker service create --network statistics --replicas 3 --name statistics-ingest -p 8081:8080 docker-registry.dmz.local/statistics-ingest-elasticsearch \
  && docker service create --network statistics --replicas 3 --name elasticsearch elasticsearch:2.3.5
```

### Lag en versjonert utgave av applikasjonen

Dette laster opp Docker-bildene til Difi sitt Docker-register. De blir samtidig tagget med et tidsstempel som
gjenspeiler byggetidspunktet.

```
$ mvn deploy
```

_Dette forutsetter tilgang til Difi sitt [Docker-register](docker-registry.dmz.local)._

## Sporbarhet

Versjonsnummeret kan hentes ved å gjøre en HTTP GET på '/'.

Fra en kjørende konteiner er det mulig å finne ut hvilket Docker-bilde i registeret applikasjonen benytter, samt
hvilken revisjon av kildekoden som er i bruk. Dette er essensielt for feilsøking og feilretting.
 
Docker-bildet finnes slik (gitt at konteineren heter ``statistics-api``:
```
$ docker ps --filter="name=statistics-api" --format="{{.Image}}"
```

Kildekode-revisjonen finner du slik (også gitt at konteineren heter ``statistics-api``):
```
$ docker inspect --format={{.ContainerConfig.Labels.label}} $(docker ps --filter="name=statistics-api" --format="{{.Image}}")
```

Alternativt kan du finne kildekode-revisjonen gitt navnet på bildet:
```
$ docker inspect --format={{.ContainerConfig.Labels.label}} docker-registry.dmz.local/statistics-api:20160704142905
```

## Kontinuerlige leveranser

For å understøtte kontinuerlige leveranser benyttes Jenkins sin ['pipeline-as-code'](https://jenkins.io/solutions/pipeline/)-
funksjonalitet. Det benyttes da en _Jenkinsfile_ som ligger på roten av prosjektet og spesifiserer byggejobben. I dette
prosjektet spesifiseres det at en versjonert utgave av applikasjonen skal lages ved hver endring på _develop_-grenen.
Endringer på andre grener vil kun sette i gang et verifiseringsbygg.
