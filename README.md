# odd-exporter
exporter component of the ODD, which serves the purpose to export the data of the individual pilots into the ODD´s database.

# installation guide with docker

- mvn clean package

- docker build -t odd_exporter .


_it is highly recommended to only deploy the odd-exporter in combination with the other ODD-components. The respective docker-compose.yml for this purpose can be found in a separate repository._