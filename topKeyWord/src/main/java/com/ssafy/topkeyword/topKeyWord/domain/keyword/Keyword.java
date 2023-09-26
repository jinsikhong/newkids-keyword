package com.ssafy.topkeyword.topKeyWord.domain.keyword;

import com.ssafy.topkeyword.topKeyWord.TimeBaseEntity;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Keyword extends TimeBaseEntity {

    @Id
    @GeneratedValue
    @Column(name = "keyword_id")
    private Long id;

    @Column(updatable = false, nullable = false, length = 20)
    private String word;

    @Builder
    private Keyword(String word) {
        this.word = word;
    }
}
