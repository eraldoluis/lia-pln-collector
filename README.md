# Desambiguação de Monitoramentos do Ctrl/s
O sistema Ctrl/s permite monitorar entidades (pessoas, instituições, empresas, produtos, etc.) em redes sociais.
A criação de um monitoramento para uma entidade específica, atualmente, envolve a definição de várias buscas.
Frequentemente,
uma entidade de interesse possui um nome ambíguo,
isto é, um nome que coincide com o nome de outra entidade.
Por exemplo,
o clube de futebol São Paulo.
O termo *São Paulo* é muito usado para denominar o clube de futebol.
Entretanto,
temos outras entidades com este mesmo nome.
Particularmente,
o estado de São Paulo e sua capital possuem o mesmo nome.
Existem diversos outros exemplos que envolvem ambiguidade.
Atualmente, o Ctrl/s não oferece recursos efetivos e eficientes para tratar estes casos.

Considerando este tipo de cenário,
este projeto visa aplicar técnicas de aprendizado de máquina que facilitem a criação de monitoramentos no Ctrl/s
de tal forma que o sistema consiga filtrar automaticamente as menções de interesse para o usuário.