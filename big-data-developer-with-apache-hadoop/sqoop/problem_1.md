````text
#Para este ejercicio hay que seguir los siguientes pasos en orden:
#1. Crear el fichero 01_comando_sqoop.sql cuyo contenido sea un comando sqoop que:
#a. Lea de la tabla accounts de loudacre
#b. Extraiga s�lo las columnas acct_num, first_name, last_name, city, state, y zipcode.
#c. S�lamente para aquellas cuentas cuyo campo first_name comience por la letra D.
#d. El directorio de destino tiene que ser /accountsD. 
#e. El fichero java que se produce tiene que situarse en el directorio /tmp. 
#f. Esto se puede hacer mediante la opci�n --outdir /tmp.
#g. El resultado del comando sqoop debe almacenarse en un solo fichero.
````
````properties
spoop import \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--table accounts \
--columns "acct_num, first_name, last_name,city, state, zipcode" \
--where "first_name like 'D%'" \
--delete-target-dir \
--target-dir /loudacre/accountsD \
--outdir /tmp \
--num-mappers 8
````