################  laboratorio 7 ###########################

#install.packages("pacman")
#install.packages()

#Librerias de limpieza de datos 

pacman::p_load(haven,dplyr, stringr)


"1.0 Set Directorio"

user <- Sys.getenv("USERNAME")  # username

setwd( paste0("C:/Users/",user,"/OneDrive/Documentos/RG_Scripts_r_py_jl/Lab7") ) # set directorio


"2.0 Cargar dataset de ENAHO"

enaho01 <- read_dta("../../enaho/2020/737-Modulo01/737-Modulo01/enaho01-2020-100.dta")

  "2.1 Formato dataframe"
enaho01 <- data.frame(read_dta("../../enaho/2020/737-Modulo01/737-Modulo01/enaho01-2020-100.dta"))

"3.0 Acceder a labels"

enaho01$p110 %>% attr('labels') # value labels
  #nombre de las variables x11 = lo que significa

enaho01$p110 %>% attr('label') # var label
  #nombre de la columna X1 = descripción de la columna


"4.0 Cargar directorios adicionales"
"Módulo02"

enaho02 = data.frame(
  read_dta("../../enaho/2020/737-Modulo01/737-Modulo01/enaho01-2020-100.dta")
)

enaho03 = data.frame(
  read_dta("../../enaho/2020/737-Modulo03/737-Modulo03/enaho01a-2020-300.dta")
)


enaho04 = data.frame(
  read_dta("../../enaho/2020/737-Modulo04/737-Modulo04/enaho01a-2020-400.dta")
)

enaho05 = data.frame(
  read_dta("../../enaho/2020/737-Modulo05/737-Modulo05/enaho01a-2020-500.dta")
)

enaho34 = data.frame(
  read_dta("../../enaho/2020/737-Modulo34/737-Modulo34/sumaria-2020.dta")
)

enaho37 = data.frame(
  read_dta("../../enaho/2020/737-Modulo37/737-Modulo37/enaho01-2020-700.dta")
)


"5.0 Seleccionar variables"
  #mantener solo variables indicadas en c(....,....,...)

enaho02 <- enaho02[ , c("conglome", "vivienda", "hogar" ,
                       "ubigeo", "dominio" ,"estrato" , "p208a", "p209",
                       "p207", "p203", "p201p" , "p204",  "facpob07")]

enaho03 <- enaho03[ , c("conglome", "vivienda", "hogar" , "codperso",
                        "p301a", "p301b", "p301c" , "p300a")]

enaho05 <- enaho05[ , c("conglome", "vivienda", "hogar" , "codperso",
                        "i524e1", "i538e1", "p558a5" , "i513t", "i518",
                        "p507", "p511a", "p512b", "p513a1", "p505" , "p506", "d544t", "d556t1",
                        "d556t2" , "d557t" , "d558t" , "ocu500" , "i530a" , "i541a")]


" 6.0 Merge section (master = x , user= y) "
# by: variable que permite identificar las observaciones en común en las bases de datos
# T = Todos


" _merge3 == 1"

enaho_merge_left <- merge(enaho02, enaho03,
                   by = c("conglome", "vivienda", "hogar"),
                   all.x = T
                   )

# all.x = T : Preserva todas las observaciones de la izquierda 
# all.x = F  valor predeterminado 


" _merge3 == 2"

enaho_merge_right <- merge(enaho02, enaho05,
                     by = c("conglome", "vivienda", "hogar"),
                     all.y = T
                      )
# all.y = T : Preserva todos los valores de base derecha


" _merge3 == 3 (match inner) = solo la intercepción de data"

enaho_merge_inner <- merge(enaho02, enaho01,
                     by = c("conglome", "vivienda", "hogar"),
                     all.x = F, all.y = F
                        )

enaho_merge_inner2 <- merge(enaho02, enaho01,
                     by = c("conglome", "vivienda", "hogar")
                        )


" Match outer = todos los valores de ambas datas" 

enaho_merge_outer1 <- merge(enaho02, enaho37,
                     by = c("conglome", "vivienda", "hogar"),
                     all = T
                        )

enaho_merge_outer2 <- merge(enaho02, enaho05,
                           by = c("conglome", "vivienda", "hogar", "codperso"),
                           all.x= T, all.y = T
                        )

# Sufijos para crear nombres de columnas únicos

enaho_merge_s1 <- merge(enaho02, enaho01,
                     by = c("conglome", "vivienda", "hogar"),
                     all.x = T, suffixes = c("","")
                      )

enaho_merge <- merge(enaho02, enaho01,
                     by = c("conglome", "vivienda", "hogar"),
                     all.x = T, suffixes = c("",".y")
                      )

" 7.0 Merge in loop"

# <-  shortcut Alt + - 

  # 7.1. Dataset de hogar

# Juntar base enaho01 + enaho34 + enaho37
num = list(enaho34 , enaho37) # lista de data.frames
  # es decir, la lista es [enaho34 , enaho37]

merge_hog = enaho01 # Master Data

for (i in num){

  merge_hog <- merge(merge_hog, i,
                     by = c("conglome", "vivienda", "hogar"),
                     all.x = T, suffixes = c("",".y")
                     )
}

names(merge_hog)

  # 7.2. Dataset individual

num = list( enaho04, enaho05 ) # lista de data.frames

merge_ind = enaho03 # Master Data

for (i in num){
  
  merge_ind <- merge(merge_ind, i,
                     by = c("conglome", "vivienda", "hogar","codperso"),
                     all.x = T, suffixes = c("",".y")
                      )
              }

names(merge_ind)


#----------------------------------------------------------
  #Juntar bases previas

merge_base <- merge(merge_ind, merge_hog,
                   by = c("conglome", "vivienda", "hogar"),
                   all.x = T, suffixes = c("",".y"))

index <- grep(".y$", colnames(merge_base))
  # da ubicaciones de nombres que tengan '.y$' de la lista 'colnames(merge_base)'  


merge_base_2019 <- merge_base[, - index]

colnames(merge_base)

#----------------------------------------------------------

"ENAHO 2019"

enaho01 <- data.frame(
  read_dta("../../../datos/2019/687-Modulo01/687-Modulo01/enaho01-2019-100.dta")
)

enaho02 = data.frame(
  read_dta("../../../datos/2019/687-Modulo02/687-Modulo02/enaho01-2019-200.dta")
)

enaho03 = data.frame(
  read_dta("../../../datos/2019/687-Modulo03/687-Modulo03/enaho01a-2019-300.dta"))

enaho04 = data.frame(
  read_dta("../../../datos/2019/687-Modulo04/687-Modulo04/enaho01a-2019-400.dta")
)

enaho05 = data.frame(
  read_dta("../../../datos/2019/687-Modulo05/687-Modulo05/enaho01a-2019-500.dta")
)

enaho34 = data.frame(
  read_dta("../../../datos/2019/687-Modulo34/687-Modulo34/sumaria-2019.dta")
)

enaho37 = data.frame(
  read_dta("../../../datos/2019/687-Modulo37/687-Modulo37/enaho01-2019-700.dta")
)


# Seleccionar variables 


enaho02 <- enaho02[ , c("conglome", "vivienda", "hogar" , "codperso",
                        "ubigeo", "dominio" ,"estrato" ,"p208a", "p209",
                        "p207", "p203", "p201p" , "p204",  "facpob07")]

enaho03 <- enaho03[ , c("conglome", "vivienda", "hogar" , "codperso",
                        "p301a", "p301b", "p301c" , "p300a")]

enaho05 <- enaho05[ , c("conglome", "vivienda", "hogar" , "codperso",
                        "i524e1", "i538e1", "p558a5" , "i513t", "i518",
                        "p507", "p511a", "p512b", "p513a1", "p505" , "p506", "d544t",
                        "d556t1",
                        "d556t2" , "d557t" , "d558t" , "ocu500" , "i530a" , "i541a")]

#-------------------------------------------------------


num = list(enaho34 , enaho37) # lista de data.frames

merge_hog = enaho01 # Master Data

for (i in num){
  
  merge_hog <- merge(merge_hog, i,
                     by = c("conglome", "vivienda", "hogar"),
                     all.x = T, suffixes = c("",".y")
  )
}

# Individual dataset

num = list(enaho03 , enaho04, enaho05 ) # lista de data.frames

merge_ind = enaho02 # Master Data

for (i in num){
  
  merge_ind <- merge(merge_ind, i,
                     by = c("conglome", "vivienda", "hogar","codperso"),
                     all.x = T, suffixes = c("",".y")
  )
}

#----------------------------------------------------------

merge_base <- merge(merge_ind, merge_hog,
                    by = c("conglome", "vivienda", "hogar"),
                    all.x = T, suffixes = c("",".y"))

index <- grep(".y$", colnames(merge_base))


merge_base_2020 <- merge_base[, - index]


### Ubigeo de provincia 


merge_base_2020['ubigeo_dep'] = substr(merge_base_2020$ubigeo, 1, 2)

merge_base_2020['ubigeo_dep_2'] = paste(str_sub(merge_base_2020$ubigeo,1,2),
                                        "00", sep = "")


#----------------------- Append -----------------------------------


merge_append <-  bind_rows(merge_base_2020, merge_base_2019)

# bind_rows from dplyr library 


write_dta(merge_append, "../data/append_enaho_r.dta")