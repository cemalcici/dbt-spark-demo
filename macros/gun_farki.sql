{# İki tarih arasındaki gün farkı: bitis - baslangic. Aynı SQL'i her modelde tekrar yazmayalım. #}
{% macro gun_farki(bitis, baslangic) -%}
    datediff({{ bitis }}, {{ baslangic }})
{%- endmacro %}
