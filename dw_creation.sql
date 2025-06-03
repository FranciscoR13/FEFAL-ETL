-- Dimensões
CREATE TABLE DimEntidade (
  id_entidade INT PRIMARY KEY
);

CREATE TABLE DimGrupoFormacao (
  id_grupo_formacao INT PRIMARY KEY,
  nome_grupo VARCHAR(512),
  descrissao_grupo VARCHAR(512)
);

CREATE TABLE DimFormacao (
  id_formacao INT PRIMARY KEY,
  nome_formacao VARCHAR(512),
  id_formacao_base INT,
  id_grupo_formacao INT,
  FOREIGN KEY (id_grupo_formacao) REFERENCES DimGrupoFormacao(id_grupo_formacao)
);

CREATE TABLE DimAreaTematica (
  id_area_tematica INT PRIMARY KEY,
  nome_area VARCHAR(512)
);

CREATE TABLE DimPreferenciaEnsino (
  id_preferencia INT PRIMARY KEY,
  descricao_preferencia VARCHAR(512)
);

CREATE TABLE DimTipoDisponibilidade (
  id_tipo_disp INT PRIMARY KEY,
  descricao_tipo_disp VARCHAR(512)
);

CREATE TABLE DimHorario (
  id_horario INT PRIMARY KEY,
  descricao_horario VARCHAR(512),
  id_tipo_disp INT,
  FOREIGN KEY (id_tipo_disp) REFERENCES DimTipoDisponibilidade(id_tipo_disp)
);

-- Fato central
CREATE TABLE FatoInquerito (
  id_inquerito INT PRIMARY KEY,
  id_entidade INT,
  ano INT,
  data_submissao DATETIME,
  existe_responsavel INT,
  nome_responsavel VARCHAR(512),
  percentagem_preenchido INT,
  tempo_realizacao INT,
  FOREIGN KEY (id_entidade) REFERENCES DimEntidade(id_entidade)
);

-- Fatos auxiliares
CREATE TABLE FatoFormacaoInquerito (
  id_resposta_formacao_inquerito INT PRIMARY KEY,
  id_inquerito INT,
  id_formacao INT,
  n_formandos INT,
  FOREIGN KEY (id_inquerito) REFERENCES FatoInquerito(id_inquerito),
  FOREIGN KEY (id_formacao) REFERENCES DimFormacao(id_formacao)
);

CREATE TABLE FatoInteresseArea (
  id_resposta_interesse_area_inquerito INT PRIMARY KEY,
  id_inquerito INT,
  id_area_tematica INT,
  tem_interesse INT,
  n_formandos INT,
  comentario VARCHAR(512),
  FOREIGN KEY (id_inquerito) REFERENCES FatoInquerito(id_inquerito),
  FOREIGN KEY (id_area_tematica) REFERENCES DimAreaTematica(id_area_tematica)
);

CREATE TABLE FatoPreferenciaEnsino (
  id_resposta_preferencia_ensino_inquerito INT PRIMARY KEY,
  id_inquerito INT,
  id_preferencia INT,
  valor_preferencia INT,
  FOREIGN KEY (id_inquerito) REFERENCES FatoInquerito(id_inquerito),
  FOREIGN KEY (id_preferencia) REFERENCES DimPreferenciaEnsino(id_preferencia)
);

CREATE TABLE FatoDisponibilidadeHoraria (
  id_resposta_disponibilidade_horaria_inquerito INT PRIMARY KEY,
  id_inquerito INT,
  id_horario INT,
  tem_disponibilidade INT,
  FOREIGN KEY (id_inquerito) REFERENCES FatoInquerito(id_inquerito),
  FOREIGN KEY (id_horario) REFERENCES DimHorario(id_horario)
);
