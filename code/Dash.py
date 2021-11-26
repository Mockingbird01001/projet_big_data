from os import sep, write
import streamlit as st
import pandas as pd
import altair as alt
import folium


#Titre du Dash
st.title("Deshboard Santé ,Finance,Travail")
#Lecture des 3 CSV
Finance=pd.read_csv("../data_archive/data_emploie.csv", sep=';')
Sante=pd.read_csv("../data_archive/data_sante.csv", sep=';')
Travail=pd.read_csv("../data_archive/data_emploie.csv", sep=';')

#line_chart(Graph)

    #Finance
montant=()
Date=()
Type=()
Latitude=()
Longitude=()

    #List
Montant=Finance.Montant.tolist()
DateFinance=Finance.datePublicationDonnees.tolist()
Type=Finance.natureObjetMarche.tolist()
Latitude=Finance.lat.tolist()
Longitude=Finance.longitude.tolist()

    #DataFrame
Data_Montant=pd.DataFrame(Montant,columns=['Montant'])
Data_DateFinance=pd.DataFrame(DateFinance,columns=['Date'])
Data_Type=pd.DataFrame(Type,columns=['Type'])
df_Finance = pd.concat([Data_Montant,Data_Type], axis=1)
df_type= df_Finance.groupby(df_Finance.Type).sum()

    #Lecture
st.header("Apercu des montants des marché par type")
st.write(df_type)

    #Visu
st.header("Representation des montants des marchés par type")
st.bar_chart(df_type)

    #Map
st.header("Map des Achteurs du Marché")
t= folium.Map(location=[20,0])
for i in range(0,len(Latitude)):
    folium.Marker(location=[Latitude[i],Longitude[i]],icon=folium.Icon(icon='Home')).add_to(t)
st.map(Finance)

    #Santé
Hospi=()
Rea=()
DateSanté=()
  
  #List
Hospi=Sante.Hospitalisation.tolist()
Rea=Sante.Reanimation.tolist()
Date_Sante=Sante.Date.tolist()
  
  #DataFrame
Data_Hospi=pd.DataFrame(Hospi,columns=['Hospitalisation'])
Data_Rea=pd.DataFrame(Rea,columns=['Reanimation'])
Data_DateSante=pd.DataFrame(Date_Sante,columns=['Date'])
df_Sante = pd.concat([Data_Hospi,Data_Rea,Data_DateSante], axis=1)
HospiReaParDate=df_Sante.groupby(df_Sante.Date).sum()
  
  #Lecture
#st.write(df_Sante)
st.header("Apercu des Hospitalisation et des Reanimations en fonction du temps ")
st.write(HospiReaParDate)
  
  #Visue
st.header("Visualisation des Hospitalisation et des Reanimations en fonction du temps ")
st.line_chart(HospiReaParDate)

    #Travail
effectifssalariesbrut=()
effectifssalariescvs=()
massesalarialebrut=()
massesalarialecvs=()
DateTravail=()

    #List
effectifssalariesbrut=Travail.effectifs_salaries_brut.tolist()
effectifssalariescvs=Travail.effectifs_salaries_cvs.tolist()
massesalarialebrut=Travail.masse_salariale_brut.tolist()
massesalarialecvs=Travail.masse_salariale_cvs.tolist()
DateTravail=Travail.Date.tolist()

    #DataFrame
Data_effectifssalariesbrut=pd.DataFrame(effectifssalariesbrut,columns=['effectifssalariesbrut'])
Data_effectifssalariescvs=pd.DataFrame(effectifssalariescvs,columns=['effectifssalariescvs'])
Data_massesalarialebrut=pd.DataFrame(massesalarialebrut,columns=['massesalarialebrut'])
Data_massesalarialecvs=pd.DataFrame(massesalarialecvs,columns=['massesalarialecvs'])
Data_DateTravail=pd.DataFrame(DateTravail,columns=['Date'])
df_Travail = pd.concat([Data_effectifssalariesbrut,Data_effectifssalariescvs,Data_massesalarialebrut,Data_massesalarialecvs,Data_DateTravail], axis=1)
df_Travail1 = pd.concat([Data_effectifssalariesbrut,Data_effectifssalariescvs,Data_massesalarialebrut,Data_massesalarialecvs], axis=1)
TraParDate=df_Travail.groupby(df_Travail.Date).sum()
  
  #Lecture
st.header("Apercu des donnees Travail en foction du temps")
st.write(TraParDate)

    #Visu
st.header("Visualisation des donnees Travail en foction du temps")
st.line_chart(TraParDate)